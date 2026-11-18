"""Build typed Silver from verified Bronze; do not read raw files here.

Run: uv run python -m pipelines.silver.build_silver
A DAG can call build_silver(run_id) after build_bronze(run_id).

Policies:
* One selected file checksum per source, taken from Bronze run metadata.
* Deduplicate orders by latest updated_at, then lowest source row position.
* Deduplicate events by event_id, latest ingestion, then lowest row position.
  Distinct capture events of a split payment are retained and summed.
* UTC-aware timestamps; late events are retained, without a watermark cutoff.
* Temporal selection uses [valid_from, valid_to); overlapping versions fail QC.
* Every source row is accepted once or rejected with its original Bronze key.
* Publish all 19 Silver tables atomically; retain historical rejection logs.
"""

import json
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from uuid import uuid4

from shared.db import connect

PROJECT_ROOT = Path(__file__).resolve().parents[2]
UTC = timezone.utc
LINEAGE = {
    'source_file': 'TEXT', 'file_sha256': 'TEXT',
    'source_row_number': 'INTEGER', 'pipeline_run_id': 'TEXT',
}
# Specs are fixed program constants. No user input is interpolated into SQL names.
SPECS = {}


def spec(name, folder, key, **fields):
    SPECS[name] = {'source': f'{folder}/{name}.json', 'key': key.split(), 'fields': fields}


spec('city_reference', 'reference', 'city_id', city_id='TEXT', city_name='TEXT', country='TEXT')
spec('sales_channels', 'operational', 'channel_id', channel_id='TEXT', channel_name='TEXT')
spec('customers', 'operational', 'customer_id', customer_id='TEXT', city_id='TEXT',
     customer_segment='TEXT', created_at_utc='TIMESTAMPTZ')
spec('customer_profiles', 'operational', 'customer_id valid_from_utc',
     customer_id='TEXT', valid_from_utc='TIMESTAMPTZ', valid_to_utc='TIMESTAMPTZ',
     city_id='TEXT', customer_segment='TEXT')
spec('customer_addresses', 'operational', 'address_id valid_from_utc',
     address_id='TEXT', customer_id='TEXT', city_id='TEXT',
     valid_from_utc='TIMESTAMPTZ', valid_to_utc='TIMESTAMPTZ')
spec('products', 'operational', 'product_id', product_id='TEXT', product_name='TEXT',
     sku='TEXT', unit_price='NUMERIC(14,2)')
spec('product_categories', 'operational', 'product_id valid_from_utc',
     product_id='TEXT', valid_from_utc='TIMESTAMPTZ', valid_to_utc='TIMESTAMPTZ',
     category_id='TEXT', category_name='TEXT')
spec('stores', 'operational', 'store_id', store_id='TEXT', store_name='TEXT',
     city_id='TEXT', location_type='TEXT')
spec('promotions', 'operational', 'promotion_id', promotion_id='TEXT', promotion_code='TEXT',
     promotion_type='TEXT', discount_rate='NUMERIC(14,6)', start_date='DATE', end_date='DATE')
spec('orders', 'operational', 'order_id', order_id='TEXT', customer_id='TEXT',
     ordered_at_utc='TIMESTAMPTZ', updated_at_utc='TIMESTAMPTZ', sales_channel='TEXT',
     status='TEXT', store_id='TEXT', shipping_revenue='NUMERIC(14,2)',
     profile_valid_from_utc='TIMESTAMPTZ')
spec('order_items', 'operational', 'order_item_id', order_item_id='TEXT', order_id='TEXT',
     product_id='TEXT', quantity='INTEGER', unit_price='NUMERIC(14,2)',
     item_discount_amount='NUMERIC(14,2)', category_valid_from_utc='TIMESTAMPTZ')
spec('order_promotions', 'operational', 'order_id promotion_id', order_id='TEXT',
     promotion_id='TEXT', discount_amount='NUMERIC(14,2)')
for name, identity in [('payment_events', 'payment_id'), ('refund_events', 'refund_id'),
                       ('return_events', 'return_id'), ('support_events', 'ticket_id')]:
    fields = {'event_id': 'TEXT', identity: 'TEXT', 'order_id': 'TEXT', 'event_type': 'TEXT',
              'occurred_at_utc': 'TIMESTAMPTZ', 'ingested_at_utc': 'TIMESTAMPTZ'}
    if name in ('payment_events', 'refund_events'):
        fields['amount'] = 'NUMERIC(14,2)'
    if name == 'support_events':
        fields.update(customer_id='TEXT', reason='TEXT')
    spec(name, 'events', 'event_id', **fields)
spec('web_events', 'events', 'event_id', event_id='TEXT', session_id='TEXT', customer_id='TEXT',
     order_id='TEXT', campaign_id='TEXT', sales_channel='TEXT', event_type='TEXT',
     occurred_at_utc='TIMESTAMPTZ', ingested_at_utc='TIMESTAMPTZ')
spec('inventory_snapshots', 'inventory', 'product_id location_id snapshot_date',
     product_id='TEXT', location_id='TEXT', snapshot_date='DATE', available_quantity='INTEGER',
     reserved_quantity='INTEGER', unit_cost='NUMERIC(14,2)')
SPECS['inventory_snapshots']['source'] = 'inventory/inventory_snapshots.csv'
spec('campaign_spend', 'reference', 'spend_date sales_channel campaign_id', spend_date='DATE',
     sales_channel='TEXT', campaign_id='TEXT', spend_amount='NUMERIC(14,2)')
SPECS['campaign_spend']['source'] = 'reference/campaign_spend.csv'

OPTIONAL = {
    'valid_to_utc', 'updated_at_utc', 'store_id', 'reason', 'campaign_id',
    'profile_valid_from_utc', 'category_valid_from_utc',
}
EVENT_TYPES = {
    'payment_events': {'PAYMENT_AUTHORIZED', 'PAYMENT_CAPTURED', 'PAYMENT_FAILED'},
    'refund_events': {'REFUND_ISSUED', 'REFUND_COMPLETED'},
    'return_events': {'RETURN_REQUESTED', 'RETURN_RECEIVED', 'RETURN_CLOSED'},
    'support_events': {'SUPPORT_TICKET_CREATED', 'SUPPORT_TICKET_CLOSED'},
    'web_events': {'PAGE_VIEW', 'ADD_TO_CART', 'PURCHASE'},
}
ORDER_STATUS = {'PLACED', 'CONFIRMED', 'FULFILLED', 'CANCELLED', 'RETURNED'}
REJECTION_TYPES = dict(LINEAGE, reason_code='TEXT', reason_detail='TEXT', details='JSONB')


class InvalidRecord(ValueError):
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)


def json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f'Unsupported JSON value: {type(value).__name__}')


def as_json(value):
    return json.dumps(value, default=json_default, ensure_ascii=False, allow_nan=False)


def convert(value, kind, field, optional=False):
    if value is None or value == '':
        if optional:
            return None
        raise InvalidRecord('MISSING_REQUIRED_FIELD', field)
    if isinstance(value, bool):
        raise InvalidRecord('INVALID_TYPE', f'{field}: boolean is not a valid {kind}')
    try:
        if kind == 'TEXT':
            if not isinstance(value, str) or not value.strip():
                raise ValueError('expected nonempty text')
            return value.strip()
        if kind == 'TIMESTAMPTZ':
            result = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
            if result.tzinfo is None or result.utcoffset() is None:
                raise ValueError('timezone is required')
            return result.astimezone(UTC)
        if kind == 'DATE':
            return date.fromisoformat(str(value))
        number = Decimal(str(value))
        if not number.is_finite():
            raise ValueError('non-finite number')
        if kind == 'INTEGER':
            if number != number.to_integral_value() or not -2147483648 <= number <= 2147483647:
                raise ValueError('invalid 32-bit integer')
            return int(number)
        scale = 6 if kind == 'NUMERIC(14,6)' else 2
        number = number.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)
        if abs(number) >= Decimal(10) ** (14 - scale):
            raise ValueError('numeric overflow')
        return number
    except (ValueError, TypeError, InvalidOperation, OverflowError) as error:
        raise InvalidRecord('INVALID_TYPE', f'{field}: {error}') from error


def normalize(name, raw, run_id):
    payload = raw['payload']
    if not isinstance(payload, dict):
        raise InvalidRecord('INVALID_PAYLOAD', 'JSON record must be an object')
    values = dict(payload)
    if name in EVENT_TYPES:
        nested = payload.get('payload')
        if not isinstance(nested, dict):
            raise InvalidRecord('INVALID_PAYLOAD', 'event payload must be an object')
        # Envelope values own event identity and timestamps.
        values = {**nested, **{k: v for k, v in payload.items() if k != 'payload'}}
    if name in ('campaign_spend', 'web_events'):
        values['sales_channel'] = values.get('channel')
    row = {}
    for field, kind in SPECS[name]['fields'].items():
        optional = field in OPTIONAL or (name == 'web_events' and field in ('customer_id', 'order_id'))
        row[field] = convert(values.get(field), kind, field, optional)
    for field in ('sales_channel', 'event_type', 'status', 'channel_id', 'channel_name', 'country'):
        if field in row and row[field] is not None:
            row[field] = row[field].upper()
    if 'status' in row:
        row['status'] = {'CANCELED': 'CANCELLED'}.get(row['status'], row['status'])
        if row['status'] not in ORDER_STATUS:
            raise InvalidRecord('INVALID_STATUS', row['status'])
    if name in EVENT_TYPES and row['event_type'] not in EVENT_TYPES[name]:
        raise InvalidRecord('INVALID_EVENT_TYPE', row['event_type'])
    for field, value in row.items():
        if isinstance(value, (Decimal, int)) and value < 0:
            code = 'NEGATIVE_QUANTITY' if field.endswith('quantity') else 'NEGATIVE_AMOUNT'
            raise InvalidRecord(code, f'{field}={value}')
    if name == 'order_items' and row['quantity'] == 0:
        raise InvalidRecord('ZERO_QUANTITY', 'quantity must be positive')
    if 'valid_from_utc' in row and row['valid_to_utc'] is not None:
        if row['valid_to_utc'] <= row['valid_from_utc']:
            raise InvalidRecord('INVALID_INTERVAL', 'valid_to must be greater than valid_from')
    if name == 'promotions' and row['end_date'] < row['start_date']:
        raise InvalidRecord('INVALID_INTERVAL', 'promotion end_date precedes start_date')
    if name == 'order_items' and row['item_discount_amount'] > row['quantity'] * row['unit_price']:
        raise InvalidRecord('EXCESS_ITEM_DISCOUNT', 'discount exceeds item gross value')
    row.update({k: raw[k] for k in ('source_file', 'file_sha256', 'source_row_number')})
    row['pipeline_run_id'] = run_id
    return row


def rejection(raw, run_id, code, detail, details=None):
    return {
        'pipeline_run_id': run_id, 'source_file': raw['source_file'],
        'file_sha256': raw['file_sha256'], 'source_row_number': raw['source_row_number'],
        'reason_code': code, 'reason_detail': str(detail), 'details': details or {},
    }


def preference(name, row):
    if name == 'orders':
        return (row['updated_at_utc'] or row['ordered_at_utc'], -row['source_row_number'])
    if name in EVENT_TYPES:
        return (row['ingested_at_utc'], row['occurred_at_utc'], -row['source_row_number'])
    return (-row['source_row_number'],)


def temporal_match(rows, instant):
    candidates = [r for r in rows if r['valid_from_utc'] <= instant and
                  (r['valid_to_utc'] is None or instant < r['valid_to_utc'])]
    return max(candidates, key=lambda r: r['valid_from_utc']) if candidates else None


def transform(bronze_rows, run_id):
    """Pure transformation: input is Bronze evidence grouped by source_file."""
    accepted, rejected, counts = {}, [], {}
    indexes = {}
    temporal = {'customer_profiles': defaultdict(list), 'product_categories': defaultdict(list)}

    def require(parent, key, label):
        if key not in indexes[parent]:
            raise InvalidRecord('MISSING_REFERENCE', f'{label}={key} not found in {parent}')

    for name, cfg in SPECS.items():
        raw_rows = bronze_rows.get(cfg['source'], [])
        parsed = []
        for raw in raw_rows:
            try:
                parsed.append(normalize(name, raw, run_id))
            except InvalidRecord as error:
                rejected.append(rejection(raw, run_id, error.code, error))
        winners = {}
        for row in sorted(parsed, key=lambda r: preference(name, r), reverse=True):
            key = tuple(row[k] for k in cfg['key'])
            if key in winners:
                rejected.append(rejection(row, run_id, 'DUPLICATE_BUSINESS_KEY', str(key), {
                    'winner_source_row_number': winners[key]['source_row_number'],
                    'policy': 'latest_update_or_ingestion_then_lowest_source_row',
                }))
            else:
                winners[key] = row
        rows = []
        for row in winners.values():
            try:
                if name in ('customers', 'customer_profiles', 'customer_addresses', 'stores'):
                    require('city_reference', row['city_id'], 'city_id')
                if name in ('customer_profiles', 'customer_addresses', 'orders', 'support_events'):
                    require('customers', row['customer_id'], 'customer_id')
                if name in ('product_categories', 'order_items', 'inventory_snapshots'):
                    require('products', row['product_id'], 'product_id')
                if name in ('order_items', 'order_promotions', 'payment_events', 'refund_events',
                            'return_events', 'support_events'):
                    require('orders', row['order_id'], 'order_id')
                if name in ('orders', 'web_events', 'campaign_spend'):
                    require('sales_channels', row['sales_channel'], 'sales_channel')
                if name == 'order_promotions':
                    require('promotions', row['promotion_id'], 'promotion_id')
                if name == 'orders':
                    if row['store_id'] is not None:
                        require('stores', row['store_id'], 'store_id')
                    version = temporal_match(temporal['customer_profiles'][row['customer_id']],
                                             row['ordered_at_utc'])
                    if version is None:
                        raise InvalidRecord('MISSING_TEMPORAL_VERSION', 'customer profile at order time')
                    row['profile_valid_from_utc'] = version['valid_from_utc']
                if name == 'order_items':
                    order = indexes['orders'][row['order_id']]
                    version = temporal_match(temporal['product_categories'][row['product_id']],
                                             order['ordered_at_utc'])
                    if version is None:
                        raise InvalidRecord('MISSING_TEMPORAL_VERSION', 'product category at order time')
                    row['category_valid_from_utc'] = version['valid_from_utc']
                if name == 'support_events':
                    if indexes['orders'][row['order_id']]['customer_id'] != row['customer_id']:
                        raise InvalidRecord('REFERENCE_MISMATCH', 'support customer differs from order customer')
                if name == 'web_events':
                    if row['customer_id'] is not None:
                        require('customers', row['customer_id'], 'customer_id')
                    if row['order_id'] is not None:
                        require('orders', row['order_id'], 'order_id')
                        if row['customer_id'] is not None:
                            if indexes['orders'][row['order_id']]['customer_id'] != row['customer_id']:
                                raise InvalidRecord('REFERENCE_MISMATCH', 'web customer differs from order customer')
                rows.append(row)
            except InvalidRecord as error:
                rejected.append(rejection(row, run_id, error.code, error))
        # Resolve entity identity without collapsing distinct event IDs.
        identity = {'payment_events': 'payment_id', 'refund_events': 'refund_id',
                    'return_events': 'return_id', 'support_events': 'ticket_id'}.get(name)
        if identity:
            owners = defaultdict(set)
            for row in rows:
                owners[row[identity]].add(row['order_id'])
            bad = {key for key, order_ids in owners.items() if len(order_ids) > 1}
            valid_rows = []
            for row in rows:
                if row[identity] in bad:
                    rejected.append(rejection(row, run_id, 'IDENTITY_CONFLICT',
                                              f'{identity} points to multiple orders'))
                else:
                    valid_rows.append(row)
            rows = valid_rows
        accepted[name] = rows
        if len(cfg['key']) == 1:
            indexes[name] = {r[cfg['key'][0]]: r for r in rows}
        if name in temporal:
            entity = 'customer_id' if name == 'customer_profiles' else 'product_id'
            for row in rows:
                temporal[name][row[entity]].append(row)
        rejected_count = sum(r['source_file'] == cfg['source'] for r in rejected)
        counts[name] = {'source_file': cfg['source'], 'input': len(raw_rows),
                        'accepted': len(rows), 'rejected': rejected_count}
        assert len(raw_rows) == len(rows) + rejected_count, f'Unaccounted records: {name}'

    # Audit exact evidence coverage, beyond merely comparing totals.
    evidence_key = lambda r: (r['source_file'], r['file_sha256'], r['source_row_number'])
    original = [evidence_key(r) for rows in bronze_rows.values() for r in rows]
    classified = ([evidence_key(r) for rows in accepted.values() for r in rows] +
                  [evidence_key(r) for r in rejected])
    if Counter(original) != Counter(classified) or len(classified) != len(set(classified)):
        raise ValueError('Bronze-to-Silver evidence coverage is not exactly one classification per row')
    return accepted, rejected, counts


def read_bronze_snapshot(run_id, bronze_run_id=None):
    """Use current DAG Bronze metadata, or the latest successful standalone Bronze run."""
    with connect('local') as connection:
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        chosen_id = bronze_run_id or run_id
        result = connection.execute('SELECT metadata FROM ops.pipeline_runs WHERE pipeline_run_id = %s',
                                    (chosen_id,)).fetchone()
        metadata = result[0] if result else {}
        if isinstance(metadata, str):
            metadata = json.loads(metadata)
        files = (metadata or {}).get('bronze', {}).get('input_files')
        if not files:
            if bronze_run_id is not None:
                raise ValueError('The requested Bronze run has no selected input-file metadata')
            result = connection.execute("""
                SELECT pipeline_run_id, metadata FROM ops.pipeline_runs
                WHERE status = 'SUCCESS' AND metadata ? 'bronze'
                ORDER BY finished_at DESC NULLS LAST, started_at DESC, pipeline_run_id
                LIMIT 1
            """).fetchone()
            if not result:
                raise ValueError('Jalankan build_bronze terlebih dahulu sampai SUCCESS.')
            chosen_id, metadata = result
            if isinstance(metadata, str):
                metadata = json.loads(metadata)
            files = metadata.get('bronze', {}).get('input_files')
        expected = {cfg['source'] for cfg in SPECS.values()}
        if not files or len(files) != len(expected) or {f['source_file'] for f in files} != expected:
            raise ValueError('Bronze metadata must specify exactly the 19 required source files')
        grouped = {}
        for source in files:
            records = connection.execute("""
                SELECT source_row_number, payload FROM bronze.raw_records
                WHERE source_file = %s AND file_sha256 = %s ORDER BY source_row_number
            """, (source['source_file'], source['file_sha256'])).fetchall()
            if len(records) != source['record_count']:
                raise ValueError(f"Bronze row count changed: {source['source_file']}")
            grouped[source['source_file']] = [{
                'source_file': source['source_file'], 'file_sha256': source['file_sha256'],
                'source_row_number': number,
                'payload': json.loads(payload) if isinstance(payload, str) else payload,
            } for number, payload in records]
        return chosen_id, files, grouped


def bulk_insert(connection, name, rows, types):
    if not rows:
        return
    columns = ', '.join(types)
    definitions = ', '.join(f'{column} {kind}' for column, kind in types.items())
    query = (f'INSERT INTO silver.{name} ({columns}) '
             f'SELECT {columns} FROM jsonb_to_recordset(%s::jsonb) AS x({definitions})')
    for start in range(0, len(rows), 2000):
        connection.execute(query, (as_json(rows[start:start + 2000]),))


STATE_VIEWS = {
    'payment_state': """
        SELECT payment_id, order_id,
            COALESCE(SUM(amount) FILTER (WHERE event_type = 'PAYMENT_CAPTURED'), 0)::NUMERIC(14,2)
                AS captured_amount,
            CASE WHEN COUNT(*) FILTER (WHERE event_type = 'PAYMENT_CAPTURED') > 0 THEN 'CAPTURED'
                 ELSE (ARRAY_AGG(REPLACE(event_type, 'PAYMENT_', '') ORDER BY
                      occurred_at_utc DESC, ingested_at_utc DESC, event_id DESC))[1]
            END AS payment_status,
            MIN(occurred_at_utc) AS first_event_at_utc,
            MAX(occurred_at_utc) AS last_event_at_utc,
            MAX(ingested_at_utc) AS latest_ingested_at_utc,
            MIN(pipeline_run_id) AS pipeline_run_id
        FROM silver.payment_events GROUP BY payment_id, order_id
    """,
    'refund_state': """
        SELECT refund_id, order_id,
            COALESCE(SUM(amount) FILTER (WHERE event_type = 'REFUND_COMPLETED'), 0)::NUMERIC(14,2)
                AS completed_amount,
            CASE WHEN COUNT(*) FILTER (WHERE event_type = 'REFUND_COMPLETED') > 0 THEN 'COMPLETED'
                 ELSE 'ISSUED' END AS refund_status,
            MAX(occurred_at_utc) AS last_event_at_utc,
            MAX(ingested_at_utc) AS latest_ingested_at_utc,
            MIN(pipeline_run_id) AS pipeline_run_id
        FROM silver.refund_events GROUP BY refund_id, order_id
    """,
    'return_state': """
        SELECT return_id, order_id,
            (ARRAY_AGG(REPLACE(event_type, 'RETURN_', '') ORDER BY occurred_at_utc DESC,
                CASE event_type WHEN 'RETURN_CLOSED' THEN 3 WHEN 'RETURN_RECEIVED' THEN 2 ELSE 1 END DESC,
                ingested_at_utc DESC, event_id DESC))[1] AS return_status,
            MAX(occurred_at_utc) AS last_event_at_utc,
            MAX(ingested_at_utc) AS latest_ingested_at_utc,
            MIN(pipeline_run_id) AS pipeline_run_id
        FROM silver.return_events GROUP BY return_id, order_id
    """,
    'support_state': """
        SELECT ticket_id, order_id, customer_id,
            MIN(occurred_at_utc) FILTER (WHERE event_type = 'SUPPORT_TICKET_CREATED') AS created_at_utc,
            MAX(occurred_at_utc) FILTER (WHERE event_type = 'SUPPORT_TICKET_CLOSED') AS closed_at_utc,
            (ARRAY_AGG(REPLACE(event_type, 'SUPPORT_TICKET_', '') ORDER BY occurred_at_utc DESC,
                CASE event_type WHEN 'SUPPORT_TICKET_CLOSED' THEN 2 ELSE 1 END DESC,
                ingested_at_utc DESC, event_id DESC))[1] AS ticket_status,
            MAX(ingested_at_utc) AS latest_ingested_at_utc,
            MIN(pipeline_run_id) AS pipeline_run_id
        FROM silver.support_events GROUP BY ticket_id, order_id, customer_id
    """,
}


def quality(connection, run_id, counts):
    """Check persisted Silver inside the publication transaction."""
    checks = []
    for name, count in counts.items():
        actual = connection.execute(f'SELECT COUNT(*) FROM silver.{name}').fetchone()[0]
        rejected = connection.execute('''SELECT COUNT(*) FROM silver.rejected_records
            WHERE pipeline_run_id = %s AND source_file = %s''',
            (run_id, count['source_file'])).fetchone()[0]
        difference = abs(actual - count['accepted']) + abs(rejected - count['rejected'])
        difference += abs(count['input'] - actual - rejected)
        checks.append((f'coverage:{name}', difference, {'input': count['input'],
            'actual_value': actual + rejected, 'target_value': count['input'],
            'accepted': actual, 'rejected': rejected}))
    for name, entity in [('customer_profiles', 'customer_id'), ('product_categories', 'product_id'),
                         ('customer_addresses', 'address_id')]:
        bad = connection.execute(f'''
            SELECT COUNT(*) FROM silver.{name} a JOIN silver.{name} b
              ON a.{entity} = b.{entity} AND a.valid_from_utc < b.valid_from_utc
             AND (a.valid_to_utc IS NULL OR b.valid_from_utc < a.valid_to_utc)
        ''').fetchone()[0]
        checks.append((f'temporal_overlap:{name}', bad, {'actual_value': bad, 'target_value': 0}))
    # Validate the legacy orders table too; it predates some of the schema FKs.
    bad = connection.execute('''
        SELECT COUNT(*) FROM silver.orders o
        LEFT JOIN silver.customers c USING (customer_id)
        LEFT JOIN silver.sales_channels ch ON ch.channel_id = o.sales_channel
        LEFT JOIN silver.stores s USING (store_id)
        LEFT JOIN silver.customer_profiles p
          ON p.customer_id = o.customer_id AND p.valid_from_utc = o.profile_valid_from_utc
        WHERE c.customer_id IS NULL OR ch.channel_id IS NULL
           OR (o.store_id IS NOT NULL AND s.store_id IS NULL)
           OR p.customer_id IS NULL OR o.ordered_at_utc < p.valid_from_utc
           OR (p.valid_to_utc IS NOT NULL AND o.ordered_at_utc >= p.valid_to_utc)
           OR o.pipeline_run_id IS DISTINCT FROM %s OR o.shipping_revenue < 0
    ''', (run_id,)).fetchone()[0]
    checks.append(('orders_references_and_temporal', bad, {'actual_value': bad, 'target_value': 0}))
    for name in SPECS:
        bad = connection.execute(f'''
            SELECT COUNT(*) FROM silver.{name} s
            LEFT JOIN bronze.raw_records b USING (source_file, file_sha256, source_row_number)
            WHERE b.source_row_number IS NULL OR s.pipeline_run_id IS DISTINCT FROM %s
        ''', (run_id,)).fetchone()[0]
        checks.append((f'lineage:{name}', bad, {'actual_value': bad, 'target_value': 0}))
    for view, table, source_amount, state_amount in [
        ('payment_state', 'payment_events', 'PAYMENT_CAPTURED', 'captured_amount'),
        ('refund_state', 'refund_events', 'REFUND_COMPLETED', 'completed_amount'),
    ]:
        difference = connection.execute(f'''
            SELECT ABS((SELECT COALESCE(SUM(amount), 0) FROM silver.{table} WHERE event_type = %s)
                - (SELECT COALESCE(SUM({state_amount}), 0) FROM silver.{view}))
        ''', (source_amount,)).fetchone()[0]
        checks.append((f'reconcile:{view}', int(difference != 0),
                       {'actual_value': str(difference), 'target_value': '0.00'}))
    for name, failed, detail in checks:
        connection.execute('''
            INSERT INTO ops.quality_results
                (pipeline_run_id, stage, check_name, status, failed_record_count, details)
            VALUES (%s, 'silver', %s, %s, %s, %s::jsonb)
            ON CONFLICT (pipeline_run_id, stage, check_name) DO UPDATE SET
                status = EXCLUDED.status, failed_record_count = EXCLUDED.failed_record_count,
                details = EXCLUDED.details, checked_at = CURRENT_TIMESTAMP
        ''', (run_id, name, 'PASS' if failed == 0 else 'FAIL', failed, as_json(detail)))
    failed_names = [name for name, failed, detail in checks if failed]
    if failed_names:
        # The surrounding transaction rolls back publication; main records the failure.
        raise ValueError('Silver quality gate FAILED: ' + ', '.join(failed_names))
    return len(checks)


def build_silver(run_id, bronze_run_id=None):
    selected_id, files, bronze = read_bronze_snapshot(run_id, bronze_run_id)
    accepted, rejected, counts = transform(bronze, run_id)
    print(f'Bronze source run: {selected_id}')
    for name, c in counts.items():
        print(f"[SILVER] {name} | input={c['input']} | accepted={c['accepted']} | rejected={c['rejected']}")
    reasons = Counter(r['reason_code'] for r in rejected)
    summary = {'bronze_run_id': selected_id, 'input_files': files, 'tables': counts,
               'accepted_records': sum(len(r) for r in accepted.values()),
               'rejected_records': len(rejected), 'rejection_reasons': dict(reasons)}
    with connect('local') as connection:
        connection.execute("SELECT pg_advisory_xact_lock(hashtext('omnichannel_silver_build'))")
        names = ', '.join(f'silver.{name}' for name in SPECS)
        # TRUNCATE obtains the table locks; without CASCADE, unrelated tables are protected.
        connection.execute(f'TRUNCATE TABLE {names}')
        connection.execute('DELETE FROM silver.rejected_records WHERE pipeline_run_id = %s', (run_id,))
        for name, rows in accepted.items():
            bulk_insert(connection, name, rows, {**SPECS[name]['fields'], **LINEAGE})
        bulk_insert(connection, 'rejected_records', rejected, REJECTION_TYPES)
        for name, query in STATE_VIEWS.items():
            connection.execute(f'CREATE OR REPLACE VIEW silver.{name} AS {query}')
        summary['quality_checks_passed'] = quality(connection, run_id, counts)
        connection.execute('ALTER TABLE silver.orders ALTER COLUMN pipeline_run_id SET NOT NULL')
        connection.execute('''UPDATE ops.pipeline_runs SET metadata = metadata || %s::jsonb
                              WHERE pipeline_run_id = %s''', (as_json({'silver': summary}), run_id))
    print('Alasan penolakan: ' + as_json(dict(reasons)))
    print(f"Quality checks Silver: {summary['quality_checks_passed']} PASS")
    return summary


def main():
    # Run the DDL as its own explicit transaction; silver.sql contains BEGIN/COMMIT.
    with connect('local') as connection:
        connection.execute((PROJECT_ROOT / 'database/schemas/silver.sql').read_text(encoding='utf-8-sig'))
    run_id = str(uuid4())
    with connect('local') as connection:
        connection.execute('''INSERT INTO ops.pipeline_runs
            (pipeline_run_id, pipeline_name, status, metadata)
            VALUES (%s, 'silver_build', 'RUNNING', %s::jsonb)''',
            (run_id, as_json({'operation': 'normalize_verify_publish_silver'})))
    print(f'Run ID: {run_id}')
    try:
        build_silver(run_id)
        with connect('local') as connection:
            connection.execute("""UPDATE ops.pipeline_runs SET status = 'SUCCESS',
                finished_at = CURRENT_TIMESTAMP WHERE pipeline_run_id = %s""", (run_id,))
        print('Status Silver tercatat di Ops: SUCCESS')
    except Exception as error:
        with connect('local') as connection:
            connection.execute('''UPDATE ops.pipeline_runs SET status = 'FAILED',
                finished_at = CURRENT_TIMESTAMP, error_message = %s WHERE pipeline_run_id = %s''',
                (str(error), run_id))
            connection.execute('''INSERT INTO ops.quality_results
                (pipeline_run_id, stage, check_name, status, failed_record_count, details)
                VALUES (%s, 'silver', 'build_and_quality_gate', 'FAIL', 1, %s::jsonb)
                ON CONFLICT (pipeline_run_id, stage, check_name) DO UPDATE SET
                    status = 'FAIL', details = EXCLUDED.details, checked_at = CURRENT_TIMESTAMP''',
                (run_id, as_json({'error': str(error), 'publication': 'rolled_back_or_not_started'})))
        raise


if __name__ == '__main__':
    main()
