import json
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_UP
from pathlib import Path
from uuid import uuid4

from shared.db import connect
from pipelines.silver.build_silver import SPECS as SILVER_SPECS, quality as validate_silver

PROJECT_ROOT = Path(__file__).resolve().parents[2]
UTC = timezone.utc
ZERO = Decimal('0.00')
MONEY = 'NUMERIC(14,2)'
RATE = 'NUMERIC(14,4)'
RESOLVED = {'FULFILLED', 'RETURNED', 'CANCELLED'}
GOLD_TYPES = {
    'order_360': dict(order_id='TEXT', customer_id='TEXT', order_date='DATE', sales_channel='TEXT',
        store_id='TEXT', gross_merchandise_value=MONEY, discount_amount=MONEY, shipping_revenue=MONEY,
        captured_payment_amount=MONEY, refunded_amount=MONEY, net_revenue=MONEY, item_count='INTEGER',
        unit_quantity='INTEGER', order_status='TEXT', payment_status='TEXT', return_status='TEXT',
        promotion_count='INTEGER', first_order_flag='BOOLEAN', pipeline_run_id='TEXT'),
    'customer_daily': dict(customer_id='TEXT', metric_date='DATE', order_count='INTEGER', unit_quantity='INTEGER',
        gross_revenue=MONEY, net_revenue=MONEY, refund_amount=MONEY, return_count='INTEGER',
        support_contacts='INTEGER', active_channel='TEXT', new_customer_flag='BOOLEAN',
        repeat_customer_flag='BOOLEAN', pipeline_run_id='TEXT'),
    'product_daily': dict(product_id='TEXT', metric_date='DATE', active_category='TEXT', units_sold='INTEGER',
        order_count='INTEGER', gross_revenue=MONEY, net_revenue=MONEY, refunded_units='INTEGER',
        available_inventory='INTEGER', reserved_inventory='INTEGER', stockout_flag='BOOLEAN',
        promotion_count='INTEGER', pipeline_run_id='TEXT'),
    'channel_campaign_daily': dict(metric_date='DATE', sales_channel='TEXT', campaign_id='TEXT',
        campaign_spend=MONEY, attributed_orders='INTEGER', attributed_customers='INTEGER',
        gross_revenue=MONEY, net_revenue=MONEY, refunds=MONEY, roas=RATE, conversion_rate=RATE,
        pipeline_run_id='TEXT'),
    'executive_kpis_daily': dict(metric_date='DATE', total_orders='INTEGER', gross_revenue=MONEY,
        net_revenue=MONEY, average_order_value=MONEY, refund_rate=RATE, return_rate=RATE,
        repeat_customer_rate=RATE, stockout_rate=RATE, active_customers='INTEGER',
        support_contact_rate=RATE, data_freshness_utc='TIMESTAMPTZ', pipeline_run_id='TEXT'),
}
GRAINS = {'order_360': ['order_id'], 'customer_daily': ['customer_id', 'metric_date'],
          'product_daily': ['product_id', 'metric_date'],
          'channel_campaign_daily': ['metric_date', 'sales_channel', 'campaign_id'],
          'executive_kpis_daily': ['metric_date']}
READ_TABLES = ['orders', 'order_items', 'order_promotions', 'products', 'product_categories',
               'payment_events', 'refund_events', 'return_events', 'support_events',
               'web_events', 'inventory_snapshots', 'campaign_spend']
POLICIES = {
    'calendar': 'UTC order-date cohorts; all accepted orders, including cancelled; not cash accounting',
    'discount': 'valid item discounts + order-promotion discounts; preaggregate before joins',
    'payment': 'sum distinct captured event IDs; preserve split captures',
    'refund': 'completed refund events only; assigned to original order cohort',
    'refund_rate': 'captured orders with completed refund / captured orders',
    'return_rate': 'resolved orders with any return / resolved orders; resolved=FULFILLED,RETURNED,CANCELLED',
    'campaign': 'one last non-null touch at/before order, same channel; otherwise UNATTRIBUTED',
    'conversion': 'purchasing sessions / sessions; session-start UTC day, one campaign per session',
    'allocation': 'order promotion/shipping/refund by product gross weight; largest remainder in cents',
    'itemless_order': 'retain order; disclose unallocated net, do not fabricate a product',
    'refunded_units': 'ESTIMATE, not observed units: round(order_units * capped refund/pre-refund-net ratio); allocate by units',
    'inventory': 'NULL if incomplete location coverage; stockout when complete total available=0',
    'category': 'version valid at end of UTC day',
    'support': 'distinct tickets on creation day; contact rate among customers with orders that day',
    'repeat': 'any order after first, including same-day repeats; new and repeat flags can coexist',
}


def json_default(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(type(value).__name__)


def as_json(value):
    return json.dumps(value, default=json_default, ensure_ascii=False, allow_nan=False)


def dec(value):
    return Decimal(str(value)) if value is not None else ZERO


def moment(value):
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    result = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('Timezone missing in typed Silver timestamp')
    return result.astimezone(UTC)


def day(value):
    return value if isinstance(value, date) and not isinstance(value, datetime) else date.fromisoformat(str(value)[:10])


def ratio(numerator, denominator, nullable=False, places=4):
    if not denominator:
        return None if nullable else Decimal(0).quantize(Decimal(1).scaleb(-places))
    return (Decimal(numerator) / Decimal(denominator)).quantize(
        Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)


def allocate_integer(total, weights):
    """Deterministic largest-remainder allocation; exact integer conservation."""
    if not weights:
        return {}
    if total < 0:
        return {k: -v for k, v in allocate_integer(-total, weights).items()}
    denominator = sum(weights.values())
    if denominator <= 0:
        weights = {k: Decimal(1) for k in weights}
        denominator = Decimal(len(weights))
    shares = {k: Decimal(total) * Decimal(w) / Decimal(denominator) for k, w in weights.items()}
    result = {k: int(v.to_integral_value(rounding=ROUND_FLOOR)) for k, v in shares.items()}
    remaining = total - sum(result.values())
    ordered = sorted(shares, key=lambda k: (-(shares[k] - result[k]), k))
    for k in ordered[:remaining]:
        result[k] += 1
    assert sum(result.values()) == total
    return result


def allocate_money(total, weights):
    return {k: Decimal(v) / 100 for k, v in allocate_integer(int(total * 100), weights).items()}


def silver_quality_gate(run_id=None, connection=None):
    """Public DAG callable; recheck the currently published Silver snapshot live."""
    if connection is None:
        with connect('local') as current:
            current.execute('LOCK TABLE ' + ', '.join('silver.' + n for n in SILVER_SPECS) + ' IN SHARE MODE')
            return silver_quality_gate(run_id, current)
    ids = connection.execute('SELECT DISTINCT pipeline_run_id FROM silver.orders').fetchall()
    if len(ids) != 1 or ids[0][0] is None:
        raise ValueError('Silver must contain one complete published run; build_silver first')
    selected = ids[0][0]
    if run_id is not None and selected != run_id:
        raise ValueError('The requested run is not the currently published Silver run')
    record = connection.execute('SELECT status, metadata FROM ops.pipeline_runs WHERE pipeline_run_id = %s',
                                (selected,)).fetchone()
    if not record or record[0] not in ('SUCCESS', 'RUNNING'):
        raise ValueError('Published Silver has no valid successful or active DAG run')
    metadata = json.loads(record[1]) if isinstance(record[1], str) else record[1]
    summary = metadata.get('silver', {})
    if set(summary.get('tables', {})) != set(SILVER_SPECS):
        raise ValueError('Silver input accounting metadata is incomplete')
    count = validate_silver(connection, selected, summary['tables'])
    print(f'Quality gate Silver: {count} PASS | run={selected}')
    return selected


def read_silver(connection):
    result = {}
    for name in READ_TABLES:
        rows = connection.execute(f'SELECT to_jsonb(s) FROM silver.{name} s').fetchall()
        result[name] = [json.loads(r[0]) if isinstance(r[0], str) else r[0] for r in rows]
    return result


def build_rows(source, run_id):
    """Pure transformation of typed Silver, independent of database writes."""
    items, promotions, events, web = defaultdict(list), defaultdict(list), {}, defaultdict(list)
    for row in source['order_items']:
        items[row['order_id']].append(row)
    for row in source['order_promotions']:
        promotions[row['order_id']].append(row)
    for name in ('payment_events', 'refund_events', 'return_events'):
        grouped = defaultdict(list)
        for row in source[name]:
            grouped[row['order_id']].append(row)
        events[name] = grouped
    for row in source['web_events']:
        web[row['order_id']].append(row)
    ordered = sorted(source['orders'], key=lambda r: (moment(r['ordered_at_utc']), r['order_id']))
    customer_seen, order_rows, attribution = set(), [], {}
    has_refund, has_return = {}, {}
    for row in ordered:
        oid, customer = row['order_id'], row['customer_id']
        lines, promos = items[oid], promotions[oid]
        gross = sum((dec(i['unit_price']) * i['quantity'] for i in lines), ZERO)
        discount = sum((dec(i['item_discount_amount']) for i in lines), ZERO)
        discount += sum((dec(p['discount_amount']) for p in promos), ZERO)
        payments = events['payment_events'][oid]
        captures = [e for e in payments if e['event_type'] == 'PAYMENT_CAPTURED']
        refunded = [e for e in events['refund_events'][oid] if e['event_type'] == 'REFUND_COMPLETED']
        returns = events['return_events'][oid]
        payment_status = 'CAPTURED' if captures else 'UNPAID'
        if payments and not captures:
            latest = max(payments, key=lambda e: (moment(e['occurred_at_utc']),
                        moment(e['ingested_at_utc']), e['event_id']))
            payment_status = latest['event_type'].removeprefix('PAYMENT_')
        return_status = 'NONE'
        if returns:
            precedence = {'RETURN_REQUESTED': 1, 'RETURN_RECEIVED': 2, 'RETURN_CLOSED': 3}
            latest = max(returns, key=lambda e: (moment(e['occurred_at_utc']), precedence[e['event_type']],
                         moment(e['ingested_at_utc']), e['event_id']))
            return_status = latest['event_type'].removeprefix('RETURN_')
        refund = sum((dec(e['amount']) for e in refunded), ZERO)
        shipping = dec(row['shipping_revenue'])
        order_rows.append(dict(order_id=oid, customer_id=customer, order_date=moment(row['ordered_at_utc']).date(),
            sales_channel=row['sales_channel'], store_id=row['store_id'], gross_merchandise_value=gross,
            discount_amount=discount, shipping_revenue=shipping,
            captured_payment_amount=sum((dec(e['amount']) for e in captures), ZERO), refunded_amount=refund,
            net_revenue=gross-discount+shipping-refund, item_count=len(lines),
            unit_quantity=sum(i['quantity'] for i in lines), order_status=row['status'],
            payment_status=payment_status, return_status=return_status, promotion_count=len(promos),
            first_order_flag=customer not in customer_seen, pipeline_run_id=run_id))
        customer_seen.add(customer)
        has_refund[oid], has_return[oid] = bool(refunded), bool(returns)
        touches = [e for e in web[oid] if e['campaign_id'] and e['sales_channel'] == row['sales_channel']
                   and moment(e['occurred_at_utc']) <= moment(row['ordered_at_utc'])]
        touch = max(touches, key=lambda e: (moment(e['occurred_at_utc']),
                    moment(e['ingested_at_utc']), e['event_id'])) if touches else None
        attribution[oid] = touch['campaign_id'] if touch else 'UNATTRIBUTED'

    support = defaultdict(list)
    tickets = defaultdict(list)
    for event in source['support_events']:
        if event['event_type'] == 'SUPPORT_TICKET_CREATED':
            tickets[event['ticket_id']].append(event)
    for ticket_events in tickets.values():
        first = min(ticket_events, key=lambda e: (moment(e['occurred_at_utc']), e['event_id']))
        support[(first['customer_id'], moment(first['occurred_at_utc']).date())].append(first['ticket_id'])
    customer_orders = defaultdict(list)
    for order in order_rows:
        customer_orders[(order['customer_id'], order['order_date'])].append(order)
    customers = []
    for customer, metric_date in sorted(set(customer_orders) | set(support)):
        orders = customer_orders[(customer, metric_date)]
        channel_value = defaultdict(lambda: ZERO)
        for order in orders:
            channel_value[order['sales_channel']] += order['net_revenue']
        active_channel = min(channel_value, key=lambda c: (-channel_value[c], c)) if channel_value else None
        customers.append(dict(customer_id=customer, metric_date=metric_date, order_count=len(orders),
            unit_quantity=sum(o['unit_quantity'] for o in orders),
            gross_revenue=sum((o['gross_merchandise_value'] for o in orders), ZERO),
            net_revenue=sum((o['net_revenue'] for o in orders), ZERO),
            refund_amount=sum((o['refunded_amount'] for o in orders), ZERO),
            return_count=sum(has_return[o['order_id']] for o in orders),
            support_contacts=len(support[(customer, metric_date)]), active_channel=active_channel,
            new_customer_flag=any(o['first_order_flag'] for o in orders),
            repeat_customer_flag=any(not o['first_order_flag'] for o in orders), pipeline_run_id=run_id))

    sales = {}
    unallocated, estimated_order_count = [], 0
    for order in order_rows:
        oid = order['order_id']
        grouped = defaultdict(list)
        for item in items[oid]:
            grouped[item['product_id']].append(item)
        if not grouped:
            unallocated.append({'order_id': oid, 'net_revenue': order['net_revenue']})
            continue
        gross = {p: sum((dec(i['unit_price']) * i['quantity'] for i in rows), ZERO) for p, rows in grouped.items()}
        units = {p: sum(i['quantity'] for i in rows) for p, rows in grouped.items()}
        weights = gross if sum(gross.values()) > 0 else units
        promo_alloc = allocate_money(sum((dec(p['discount_amount']) for p in promotions[oid]), ZERO), weights)
        ship_alloc = allocate_money(order['shipping_revenue'], weights)
        refund_alloc = allocate_money(order['refunded_amount'], weights)
        pre_refund_net = order['net_revenue'] + order['refunded_amount']
        fraction = min(order['refunded_amount'] / pre_refund_net, Decimal(1)) if pre_refund_net > 0 else Decimal(int(order['refunded_amount'] > 0))
        estimated_total = int((Decimal(order['unit_quantity']) * fraction).quantize(Decimal(1), rounding=ROUND_HALF_UP))
        estimated = allocate_integer(estimated_total, units)
        estimated_order_count += int(order['refunded_amount'] > 0)
        allocated_net = ZERO
        for product, lines in grouped.items():
            key = (product, order['order_date'])
            if key not in sales:
                sales[key] = {'units': 0, 'orders': set(), 'gross': ZERO, 'net': ZERO,
                              'refunded_units': 0, 'promotions': set()}
            entry = sales[key]
            net = gross[product] - sum((dec(i['item_discount_amount']) for i in lines), ZERO)
            net += -promo_alloc[product] + ship_alloc[product] - refund_alloc[product]
            allocated_net += net
            entry['units'] += units[product]
            entry['orders'].add(oid)
            entry['gross'] += gross[product]
            entry['net'] += net
            entry['refunded_units'] += estimated[product]
            entry['promotions'].update((oid, p['promotion_id']) for p in promotions[oid])
        if allocated_net != order['net_revenue']:
            raise ValueError(f'Cent allocation mismatch: {oid}')

    inventory = defaultdict(list)
    locations = {r['location_id'] for r in source['inventory_snapshots']}
    for row in source['inventory_snapshots']:
        inventory[(row['product_id'], day(row['snapshot_date']))].append(row)
    categories = defaultdict(list)
    for row in source['product_categories']:
        categories[row['product_id']].append(row)
    business_dates = {o['order_date'] for o in order_rows} | {d for p, d in inventory}
    if not business_dates:
        raise ValueError('No order or inventory dates to build Gold')
    first_day, last_day = min(business_dates), max(business_dates)
    dates = [first_day + timedelta(days=i) for i in range((last_day-first_day).days + 1)]
    product_rows, missing_inventory = [], 0
    for product in sorted(p['product_id'] for p in source['products']):
        for metric_date in dates:
            key = (product, metric_date)
            entry = sales.get(key, {'units': 0, 'orders': set(), 'gross': ZERO, 'net': ZERO,
                                    'refunded_units': 0, 'promotions': set()})
            snapshots = inventory[key]
            complete = bool(locations) and {r['location_id'] for r in snapshots} == locations
            available = sum(r['available_quantity'] for r in snapshots) if complete else None
            reserved = sum(r['reserved_quantity'] for r in snapshots) if complete else None
            missing_inventory += int(not complete)
            end_of_day = datetime.combine(metric_date + timedelta(days=1), time.min, UTC) - timedelta(microseconds=1)
            versions = [r for r in categories[product] if moment(r['valid_from_utc']) <= end_of_day and
                        (r['valid_to_utc'] is None or end_of_day < moment(r['valid_to_utc']))]
            category = max(versions, key=lambda r: moment(r['valid_from_utc']))['category_name'] if versions else None
            product_rows.append(dict(product_id=product, metric_date=metric_date, active_category=category,
                units_sold=entry['units'], order_count=len(entry['orders']), gross_revenue=entry['gross'],
                net_revenue=entry['net'], refunded_units=entry['refunded_units'], available_inventory=available,
                reserved_inventory=reserved, stockout_flag=(available == 0 if complete else None),
                promotion_count=len(entry['promotions']), pipeline_run_id=run_id))

    buckets = defaultdict(list)
    for order in order_rows:
        buckets[(order['order_date'], order['sales_channel'], attribution[order['order_id']])].append(order)
    spend = defaultdict(lambda: ZERO)
    for row in source['campaign_spend']:
        spend[(day(row['spend_date']), row['sales_channel'], row['campaign_id'])] += dec(row['spend_amount'])
    session_events = defaultdict(list)
    for row in source['web_events']:
        session_events[(row['sales_channel'], row['session_id'])].append(row)
    sessions = defaultdict(lambda: [0, 0])
    for (channel, session), rows in session_events.items():
        starts = min(moment(r['occurred_at_utc']) for r in rows)
        purchases = [moment(r['occurred_at_utc']) for r in rows if r['event_type'] == 'PURCHASE']
        cutoff = min(purchases) if purchases else max(moment(r['occurred_at_utc']) for r in rows)
        touches = [r for r in rows if r['campaign_id'] and moment(r['occurred_at_utc']) <= cutoff]
        latest = max(touches, key=lambda r: (moment(r['occurred_at_utc']), moment(r['ingested_at_utc']), r['event_id'])) if touches else None
        campaign = latest['campaign_id'] if latest else 'UNATTRIBUTED'
        key = (starts.date(), channel, campaign)
        sessions[key][0] += 1
        sessions[key][1] += int(bool(purchases))
    channels = []
    for key in sorted(set(buckets) | set(spend) | set(sessions)):
        metric_date, channel, campaign = key
        orders = buckets[key]
        net = sum((o['net_revenue'] for o in orders), ZERO)
        channels.append(dict(metric_date=metric_date, sales_channel=channel, campaign_id=campaign,
            campaign_spend=spend[key], attributed_orders=len(orders),
            attributed_customers=len({o['customer_id'] for o in orders}),
            gross_revenue=sum((o['gross_merchandise_value'] for o in orders), ZERO), net_revenue=net,
            refunds=sum((o['refunded_amount'] for o in orders), ZERO), roas=ratio(net, spend[key], True),
            conversion_rate=ratio(sessions[key][1], sessions[key][0], True), pipeline_run_id=run_id))

    freshness_values = [moment(o['updated_at_utc'] or o['ordered_at_utc']) for o in source['orders']]
    for name in ('payment_events', 'refund_events', 'return_events', 'support_events', 'web_events'):
        freshness_values.extend(moment(e['ingested_at_utc']) for e in source[name])
    freshness = max(freshness_values)
    daily_orders, daily_customers, daily_products = defaultdict(list), defaultdict(list), defaultdict(list)
    for row in order_rows: daily_orders[row['order_date']].append(row)
    for row in customers: daily_customers[row['metric_date']].append(row)
    for row in product_rows: daily_products[row['metric_date']].append(row)
    all_dates = set(daily_orders) | set(daily_customers) | set(daily_products) | {r['metric_date'] for r in channels}
    first_day, last_day = min(all_dates), max(all_dates)
    executive = []
    for i in range((last_day-first_day).days + 1):
        metric_date = first_day + timedelta(days=i)
        orders = daily_orders[metric_date]
        captured = [o for o in orders if o['payment_status'] == 'CAPTURED']
        resolved = [o for o in orders if o['order_status'] in RESOLVED]
        active = [c for c in daily_customers[metric_date] if c['order_count'] > 0]
        observed = [p for p in daily_products[metric_date] if p['stockout_flag'] is not None]
        net = sum((o['net_revenue'] for o in orders), ZERO)
        executive.append(dict(metric_date=metric_date, total_orders=len(orders),
            gross_revenue=sum((o['gross_merchandise_value'] for o in orders), ZERO), net_revenue=net,
            average_order_value=ratio(net, len(orders), places=2),
            refund_rate=ratio(sum(has_refund[o['order_id']] for o in captured), len(captured)),
            return_rate=ratio(sum(has_return[o['order_id']] for o in resolved), len(resolved)),
            repeat_customer_rate=ratio(sum(c['repeat_customer_flag'] for c in active), len(active)),
            stockout_rate=ratio(sum(p['stockout_flag'] for p in observed), len(observed)),
            active_customers=len(active),
            support_contact_rate=ratio(sum(c['support_contacts'] > 0 for c in active), len(active)),
            data_freshness_utc=freshness, pipeline_run_id=run_id))
    result = {'order_360': order_rows, 'customer_daily': customers, 'product_daily': product_rows,
              'channel_campaign_daily': channels, 'executive_kpis_daily': executive}
    diagnostics = {'itemless_orders': unallocated,
        'unallocated_product_net': sum((r['net_revenue'] for r in unallocated), ZERO),
        'missing_inventory_product_days': missing_inventory, 'inventory_locations': sorted(locations),
        'refunded_unit_estimated_orders': estimated_order_count,
        'cancelled_captured_orders': sum(o['order_status'] == 'CANCELLED' and o['payment_status'] == 'CAPTURED' for o in order_rows),
        'source_freshness_utc': freshness}
    return result, diagnostics


def insert_rows(connection, name, rows):
    types = GOLD_TYPES[name]
    columns = ', '.join(types)
    definitions = ', '.join(f'{k} {v}' for k, v in types.items())
    query = f'INSERT INTO gold.{name} ({columns}) SELECT {columns} FROM jsonb_to_recordset(%s::jsonb) AS x({definitions})'
    for offset in range(0, len(rows), 2000):
        connection.execute(query, (as_json(rows[offset:offset+2000]),))


def gold_quality(connection, run_id, expected, diagnostics):
    """Independent SQL comparisons against Silver and across Gold grains."""
    checks = []
    def check(name, sql, params=()):
        bad = connection.execute(sql, params).fetchone()[0]
        checks.append((name, 'PASS' if bad == 0 else 'FAIL', int(bad),
                       {'actual_value': str(bad), 'target_value': '0'}))
    for name, grain in GRAINS.items():
        check(f'grain:{name}', f'SELECT COUNT(*) FROM (SELECT {", ".join(grain)} FROM gold.{name} GROUP BY {", ".join(grain)} HAVING COUNT(*) > 1) x')
        check(f'row_count:{name}', f'SELECT ABS(COUNT(*) - %s) FROM gold.{name}', (len(expected[name]),))
        check(f'run_id:{name}', f'SELECT COUNT(*) FROM gold.{name} WHERE pipeline_run_id IS DISTINCT FROM %s', (run_id,))
    check('order_coverage', '''SELECT COUNT(*) FROM silver.orders s FULL JOIN gold.order_360 g USING(order_id)
        WHERE s.order_id IS NULL OR g.order_id IS NULL''')
    check('order_values_from_silver', '''
        WITH items AS (SELECT order_id, SUM(quantity * unit_price) AS gross,
            SUM(item_discount_amount) AS discount, COUNT(*) AS items, SUM(quantity) AS units
            FROM silver.order_items GROUP BY order_id),
        promos AS (SELECT order_id, SUM(discount_amount) AS discount, COUNT(*) AS promos
            FROM silver.order_promotions GROUP BY order_id)
        SELECT COUNT(*) FROM gold.order_360 g JOIN silver.orders s USING(order_id)
        LEFT JOIN items i USING(order_id) LEFT JOIN promos p USING(order_id)
        WHERE g.gross_merchandise_value <> COALESCE(i.gross,0)
           OR g.discount_amount <> COALESCE(i.discount,0) + COALESCE(p.discount,0)
           OR g.shipping_revenue <> s.shipping_revenue
           OR g.item_count <> COALESCE(i.items,0) OR g.unit_quantity <> COALESCE(i.units,0)
           OR g.promotion_count <> COALESCE(p.promos,0)
           OR g.order_date <> (s.ordered_at_utc AT TIME ZONE 'UTC')::DATE
           OR g.customer_id <> s.customer_id OR g.sales_channel <> s.sales_channel
           OR g.store_id IS DISTINCT FROM s.store_id OR g.order_status <> s.status
    ''')
    check('order_net_formula', '''SELECT COUNT(*) FROM gold.order_360
        WHERE net_revenue <> gross_merchandise_value-discount_amount+shipping_revenue-refunded_amount''')
    check('captured_payments', '''WITH p AS (SELECT order_id, SUM(amount) AS amount
        FROM silver.payment_events WHERE event_type='PAYMENT_CAPTURED' GROUP BY order_id)
        SELECT COUNT(*) FROM gold.order_360 g LEFT JOIN p USING(order_id)
        WHERE g.captured_payment_amount <> COALESCE(p.amount,0)
           OR (g.payment_status='CAPTURED') IS DISTINCT FROM (p.order_id IS NOT NULL)''')
    check('completed_refunds', '''WITH r AS (SELECT order_id, SUM(amount) AS amount
        FROM silver.refund_events WHERE event_type='REFUND_COMPLETED' GROUP BY order_id)
        SELECT COUNT(*) FROM gold.order_360 g LEFT JOIN r USING(order_id)
        WHERE g.refunded_amount <> COALESCE(r.amount,0)''')
    check('first_order_flags', '''WITH ranked AS (SELECT order_id, ROW_NUMBER() OVER
        (PARTITION BY customer_id ORDER BY ordered_at_utc, order_id) AS seq FROM silver.orders)
        SELECT COUNT(*) FROM gold.order_360 g JOIN ranked USING(order_id)
        WHERE g.first_order_flag IS DISTINCT FROM (ranked.seq=1)''')
    for name in ('customer_daily', 'channel_campaign_daily', 'executive_kpis_daily'):
        check(f'net_total:{name}', f'''SELECT CASE WHEN
            (SELECT COALESCE(SUM(net_revenue),0) FROM gold.{name}) =
            (SELECT COALESCE(SUM(net_revenue),0) FROM gold.order_360) THEN 0 ELSE 1 END''')
    for name in ('customer_daily', 'product_daily', 'channel_campaign_daily', 'executive_kpis_daily'):
        check(f'gross_total:{name}', f'''SELECT CASE WHEN
            (SELECT COALESCE(SUM(gross_revenue),0) FROM gold.{name}) =
            (SELECT COALESCE(SUM(gross_merchandise_value),0) FROM gold.order_360) THEN 0 ELSE 1 END''')
    check('product_net_with_disclosed_unallocated', '''SELECT CASE WHEN
        (SELECT COALESCE(SUM(net_revenue),0) FROM gold.order_360) -
        (SELECT COALESCE(SUM(net_revenue),0) FROM gold.product_daily) =
        (SELECT COALESCE(SUM(g.net_revenue),0) FROM gold.order_360 g
         WHERE NOT EXISTS (SELECT 1 FROM silver.order_items i WHERE i.order_id=g.order_id))
        THEN 0 ELSE 1 END''')
    check('campaign_spend_total', '''SELECT CASE WHEN
        (SELECT COALESCE(SUM(campaign_spend),0) FROM gold.channel_campaign_daily) =
        (SELECT COALESCE(SUM(spend_amount),0) FROM silver.campaign_spend) THEN 0 ELSE 1 END''')
    check('campaign_order_count', '''SELECT ABS(
        (SELECT COALESCE(SUM(attributed_orders),0) FROM gold.channel_campaign_daily) -
        (SELECT COUNT(*) FROM gold.order_360))''')
    check('customer_order_count', '''SELECT ABS(
        (SELECT COALESCE(SUM(order_count),0) FROM gold.customer_daily) -
        (SELECT COUNT(*) FROM gold.order_360))''')
    check('executive_order_count', '''SELECT ABS(
        (SELECT COALESCE(SUM(total_orders),0) FROM gold.executive_kpis_daily) -
        (SELECT COUNT(*) FROM gold.order_360))''')
    check('product_unit_total', '''SELECT ABS(
        (SELECT COALESCE(SUM(units_sold),0) FROM gold.product_daily) -
        (SELECT COALESCE(SUM(quantity),0) FROM silver.order_items))''')
    check('customer_refund_total', '''SELECT CASE WHEN
        (SELECT COALESCE(SUM(refund_amount),0) FROM gold.customer_daily) =
        (SELECT COALESCE(SUM(refunded_amount),0) FROM gold.order_360) THEN 0 ELSE 1 END''')
    check('campaign_refund_total', '''SELECT CASE WHEN
        (SELECT COALESCE(SUM(refunds),0) FROM gold.channel_campaign_daily) =
        (SELECT COALESCE(SUM(refunded_amount),0) FROM gold.order_360) THEN 0 ELSE 1 END''')
    check('customer_support_contacts', '''SELECT ABS(
        (SELECT COALESCE(SUM(support_contacts),0) FROM gold.customer_daily) -
        (SELECT COUNT(DISTINCT ticket_id) FROM silver.support_events WHERE event_type='SUPPORT_TICKET_CREATED'))''')
    check('daily_customer_values', '''
        WITH o AS (SELECT customer_id,order_date,COUNT(*) AS orders,SUM(unit_quantity) AS units,
          SUM(gross_merchandise_value) AS gross,SUM(net_revenue) AS net,SUM(refunded_amount) AS refund,
          BOOL_OR(first_order_flag) AS is_new,BOOL_OR(NOT first_order_flag) AS is_repeat
          FROM gold.order_360 GROUP BY customer_id,order_date),
        tickets AS (SELECT customer_id,ticket_id,MIN(occurred_at_utc) AS created
          FROM silver.support_events WHERE event_type='SUPPORT_TICKET_CREATED' GROUP BY customer_id,ticket_id),
        contacts AS (SELECT customer_id,(created AT TIME ZONE 'UTC')::DATE AS metric_date,COUNT(*) AS contacts
          FROM tickets GROUP BY customer_id,(created AT TIME ZONE 'UTC')::DATE)
        SELECT COUNT(*) FROM gold.customer_daily c
        LEFT JOIN o ON o.customer_id=c.customer_id AND o.order_date=c.metric_date
        LEFT JOIN contacts t ON t.customer_id=c.customer_id AND t.metric_date=c.metric_date
        WHERE c.order_count<>COALESCE(o.orders,0) OR c.unit_quantity<>COALESCE(o.units,0)
          OR c.gross_revenue<>COALESCE(o.gross,0) OR c.net_revenue<>COALESCE(o.net,0)
          OR c.refund_amount<>COALESCE(o.refund,0) OR c.support_contacts<>COALESCE(t.contacts,0)
          OR c.new_customer_flag IS DISTINCT FROM COALESCE(o.is_new,FALSE)
          OR c.repeat_customer_flag IS DISTINCT FROM COALESCE(o.is_repeat,FALSE)
    ''')
    check('campaign_last_touch_values', '''
        WITH attributed AS (SELECT g.*,COALESCE(w.campaign_id,'UNATTRIBUTED') AS campaign_id
          FROM gold.order_360 g JOIN silver.orders o USING(order_id)
          LEFT JOIN LATERAL (SELECT campaign_id FROM silver.web_events w
            WHERE w.order_id=o.order_id AND w.sales_channel=o.sales_channel AND w.campaign_id IS NOT NULL
              AND w.occurred_at_utc<=o.ordered_at_utc
            ORDER BY w.occurred_at_utc DESC,w.ingested_at_utc DESC,w.event_id DESC LIMIT 1) w ON TRUE),
        a AS (SELECT order_date AS metric_date,sales_channel,campaign_id,COUNT(*) AS orders,
          COUNT(DISTINCT customer_id) AS customers,SUM(gross_merchandise_value) AS gross,
          SUM(net_revenue) AS net,SUM(refunded_amount) AS refunds
          FROM attributed GROUP BY order_date,sales_channel,campaign_id)
        SELECT COUNT(*) FROM gold.channel_campaign_daily g
        FULL JOIN a USING(metric_date,sales_channel,campaign_id)
        WHERE g.metric_date IS NULL OR g.attributed_orders<>COALESCE(a.orders,0)
          OR g.attributed_customers<>COALESCE(a.customers,0) OR g.gross_revenue<>COALESCE(a.gross,0)
          OR g.net_revenue<>COALESCE(a.net,0) OR g.refunds<>COALESCE(a.refunds,0)
    ''')
    check('session_conversion_rates', '''
        WITH annotated AS (SELECT w.*,MIN(occurred_at_utc) FILTER(WHERE event_type='PURCHASE')
          OVER(PARTITION BY sales_channel,session_id) AS first_purchase FROM silver.web_events w),
        sessions AS (SELECT sales_channel,session_id,(MIN(occurred_at_utc) AT TIME ZONE 'UTC')::DATE AS metric_date,
          COALESCE((ARRAY_AGG(campaign_id ORDER BY occurred_at_utc DESC,ingested_at_utc DESC,event_id DESC)
            FILTER(WHERE campaign_id IS NOT NULL AND (first_purchase IS NULL OR occurred_at_utc<=first_purchase)))[1],
            'UNATTRIBUTED') AS campaign_id,BOOL_OR(event_type='PURCHASE') AS converted
          FROM annotated GROUP BY sales_channel,session_id),
        rates AS (SELECT metric_date,sales_channel,campaign_id,
          ROUND(COUNT(*) FILTER(WHERE converted)::NUMERIC/COUNT(*),4) AS conversion
          FROM sessions GROUP BY metric_date,sales_channel,campaign_id)
        SELECT COUNT(*) FROM gold.channel_campaign_daily g
        FULL JOIN rates r USING(metric_date,sales_channel,campaign_id)
        WHERE g.metric_date IS NULL OR g.conversion_rate IS DISTINCT FROM r.conversion
    ''')
    check('daily_executive_values', '''
        WITH o AS (SELECT order_date,COUNT(*) AS orders,COUNT(DISTINCT customer_id) AS customers,
          SUM(gross_merchandise_value) AS gross,SUM(net_revenue) AS net
          FROM gold.order_360 GROUP BY order_date),
        c AS (SELECT metric_date,COUNT(*) FILTER(WHERE repeat_customer_flag) AS repeats,
          COUNT(*) FILTER(WHERE support_contacts>0) AS supported
          FROM gold.customer_daily WHERE order_count>0 GROUP BY metric_date),
        p AS (SELECT metric_date,COUNT(*) FILTER(WHERE stockout_flag IS NOT NULL) AS observed,
          COUNT(*) FILTER(WHERE stockout_flag) AS stockouts FROM gold.product_daily GROUP BY metric_date)
        SELECT COUNT(*) FROM gold.executive_kpis_daily k
        LEFT JOIN o ON o.order_date=k.metric_date
        LEFT JOIN c USING(metric_date) LEFT JOIN p USING(metric_date)
        WHERE k.total_orders<>COALESCE(o.orders,0) OR k.active_customers<>COALESCE(o.customers,0)
          OR k.gross_revenue<>COALESCE(o.gross,0) OR k.net_revenue<>COALESCE(o.net,0)
          OR k.average_order_value<>COALESCE(ROUND(o.net/NULLIF(o.orders,0),2),0)
          OR k.repeat_customer_rate<>COALESCE(ROUND(c.repeats::NUMERIC/NULLIF(o.customers,0),4),0)
          OR k.support_contact_rate<>COALESCE(ROUND(c.supported::NUMERIC/NULLIF(o.customers,0),4),0)
          OR k.stockout_rate<>COALESCE(ROUND(p.stockouts::NUMERIC/NULLIF(p.observed,0),4),0)
    ''')
    check('executive_refund_and_return_rates', '''
        WITH cohorts AS (SELECT g.order_date,
          COUNT(*) FILTER (WHERE g.payment_status='CAPTURED') AS captured,
          COUNT(*) FILTER (WHERE g.payment_status='CAPTURED' AND EXISTS
            (SELECT 1 FROM silver.refund_events r WHERE r.order_id=g.order_id AND r.event_type='REFUND_COMPLETED')) AS refunded,
          COUNT(*) FILTER (WHERE g.order_status IN ('FULFILLED','RETURNED','CANCELLED')) AS resolved,
          COUNT(*) FILTER (WHERE g.order_status IN ('FULFILLED','RETURNED','CANCELLED') AND EXISTS
            (SELECT 1 FROM silver.return_events r WHERE r.order_id=g.order_id)) AS returned
          FROM gold.order_360 g GROUP BY g.order_date)
        SELECT COUNT(*) FROM gold.executive_kpis_daily k LEFT JOIN cohorts c ON c.order_date=k.metric_date
        WHERE k.refund_rate <> COALESCE(ROUND(c.refunded::NUMERIC/NULLIF(c.captured,0),4),0)
           OR k.return_rate <> COALESCE(ROUND(c.returned::NUMERIC/NULLIF(c.resolved,0),4),0)
    ''')
    check('inventory_completeness', '''
        WITH expected AS (SELECT COUNT(DISTINCT location_id) AS locations FROM silver.inventory_snapshots),
        inv AS (SELECT product_id,snapshot_date,COUNT(DISTINCT location_id) AS locations,
          SUM(available_quantity) AS available,SUM(reserved_quantity) AS reserved
          FROM silver.inventory_snapshots GROUP BY product_id,snapshot_date)
        SELECT COUNT(*) FROM gold.product_daily g CROSS JOIN expected e
        LEFT JOIN inv i ON i.product_id=g.product_id AND i.snapshot_date=g.metric_date
        WHERE g.available_inventory IS DISTINCT FROM CASE WHEN i.locations=e.locations AND e.locations>0 THEN i.available END
           OR g.reserved_inventory IS DISTINCT FROM CASE WHEN i.locations=e.locations AND e.locations>0 THEN i.reserved END
           OR g.stockout_flag IS DISTINCT FROM CASE WHEN i.locations=e.locations AND e.locations>0 THEN i.available=0 END
    ''')
    check('product_category_as_of_day', '''SELECT COUNT(*) FROM gold.product_daily g
        LEFT JOIN LATERAL (SELECT category_name FROM silver.product_categories c
          WHERE c.product_id=g.product_id AND c.valid_from_utc <=
            ((g.metric_date+1)::TIMESTAMP AT TIME ZONE 'UTC')-INTERVAL '1 microsecond'
          AND (c.valid_to_utc IS NULL OR c.valid_to_utc >
            ((g.metric_date+1)::TIMESTAMP AT TIME ZONE 'UTC')-INTERVAL '1 microsecond')
          ORDER BY c.valid_from_utc DESC LIMIT 1) c ON TRUE
        WHERE g.active_category IS DISTINCT FROM c.category_name''')
    check('rates_and_roas', '''SELECT
        (SELECT COUNT(*) FROM gold.channel_campaign_daily WHERE
          roas IS DISTINCT FROM ROUND(net_revenue/NULLIF(campaign_spend,0),4)
          OR conversion_rate < 0 OR conversion_rate > 1) +
        (SELECT COUNT(*) FROM gold.executive_kpis_daily WHERE refund_rate NOT BETWEEN 0 AND 1
          OR return_rate NOT BETWEEN 0 AND 1 OR repeat_customer_rate NOT BETWEEN 0 AND 1
          OR stockout_rate NOT BETWEEN 0 AND 1 OR support_contact_rate NOT BETWEEN 0 AND 1)''')
    check('estimated_refund_unit_bounds', '''SELECT COUNT(*) FROM gold.product_daily
        WHERE refunded_units < 0 OR refunded_units > units_sold''')
    for name, affected, details in [
        ('disclosure:itemless_orders', len(diagnostics['itemless_orders']),
         {'orders': diagnostics['itemless_orders'], 'unallocated_product_net': diagnostics['unallocated_product_net'],
          'action': 'retained at order/customer/campaign/executive grain; no fabricated product'}),
        ('disclosure:missing_inventory', diagnostics['missing_inventory_product_days'],
         {'action': 'NULL inventory/stockout where coverage is incomplete', 'locations': diagnostics['inventory_locations']}),
        ('disclosure:estimated_refunded_units', diagnostics['refunded_unit_estimated_orders'],
         {'action': POLICIES['refunded_units'], 'source_has_observed_refunded_units': False}),
    ]:
        checks.append((name, 'WARN' if affected else 'PASS', 0,
                       {'affected_records': affected, **details}))
    return checks


def save_checks(connection, run_id, checks):
    for name, status, failed, details in checks:
        connection.execute('''INSERT INTO ops.quality_results
            (pipeline_run_id,stage,check_name,status,failed_record_count,details)
            VALUES (%s,'gold',%s,%s,%s,%s::jsonb)
            ON CONFLICT(pipeline_run_id,stage,check_name) DO UPDATE SET
              status=EXCLUDED.status,failed_record_count=EXCLUDED.failed_record_count,
              details=EXCLUDED.details,checked_at=CURRENT_TIMESTAMP''',
            (run_id,name,status,failed,as_json(details)))


class GoldQualityError(ValueError):
    def __init__(self, checks):
        self.checks = checks
        super().__init__('Gold quality gate FAILED: ' + ', '.join(c[0] for c in checks if c[1]=='FAIL'))


def build_gold(run_id, silver_run_id=None):
    try:
        with connect('local') as connection:
            connection.execute("SELECT pg_advisory_xact_lock(hashtext('omnichannel_gold_build'))")
            connection.execute('LOCK TABLE ' + ', '.join('silver.'+n for n in SILVER_SPECS) + ' IN SHARE MODE')
            selected = silver_quality_gate(silver_run_id, connection)
            source = read_silver(connection)
            rows, diagnostics = build_rows(source, run_id)
            connection.execute('TRUNCATE TABLE ' + ', '.join('gold.'+n for n in GOLD_TYPES))
            for name, records in rows.items():
                insert_rows(connection, name, records)
                print(f'[GOLD] {name}: {len(records)} baris')
            checks = gold_quality(connection, run_id, rows, diagnostics)
            if any(c[1]=='FAIL' for c in checks):
                raise GoldQualityError(checks)
            save_checks(connection, run_id, checks)
            summary = {'silver_run_id': selected, 'row_counts': {n:len(r) for n,r in rows.items()},
                'quality': dict(Counter(c[1] for c in checks)), 'diagnostics': diagnostics, 'policies': POLICIES}
            connection.execute('UPDATE ops.pipeline_runs SET metadata=metadata || %s::jsonb WHERE pipeline_run_id=%s',
                                (as_json({'gold': summary}),run_id))
        print('Quality checks Gold: ' + as_json(summary['quality']))
        for name,status,failed,detail in checks:
            if status=='WARN':
                print(f"[WARN] {name} | affected={detail['affected_records']}")
        return summary
    except GoldQualityError as error:
        with connect('local') as connection:
            save_checks(connection,run_id,error.checks)
        raise


def main():
    with connect('local') as connection:
        connection.execute((PROJECT_ROOT/'database/schemas/gold.sql').read_text(encoding='utf-8-sig'))
    run_id = str(uuid4())
    with connect('local') as connection:
        connection.execute('''INSERT INTO ops.pipeline_runs(pipeline_run_id,pipeline_name,status,metadata)
            VALUES(%s,'gold_build','RUNNING',%s::jsonb)''',
            (run_id,as_json({'operation':'build_validate_publish_gold'})))
    print(f'Run ID: {run_id}')
    try:
        build_gold(run_id)
        with connect('local') as connection:
            connection.execute("UPDATE ops.pipeline_runs SET status='SUCCESS',finished_at=CURRENT_TIMESTAMP WHERE pipeline_run_id=%s",(run_id,))
        print('Status Gold tercatat di Ops: SUCCESS')
    except Exception as error:
        with connect('local') as connection:
            connection.execute("UPDATE ops.pipeline_runs SET status='FAILED',finished_at=CURRENT_TIMESTAMP,error_message=%s WHERE pipeline_run_id=%s",(str(error),run_id))
            save_checks(connection,run_id,[('build_and_quality_gate','FAIL',1,{'error':str(error),'publication':'rolled_back_or_not_started'})])
        raise


if __name__ == '__main__':
    main()
