"""AIEN milestone: run the complete local retail pipeline from this DAG.

Required files: participant ops/bronze/silver schemas and three build modules,
plus the supplied Gold contract and shared.db. No manifest.json is used.
No database connection, file ingestion or processing occurs at DAG parse time.
One deterministic pipeline_run_id is shared across all tasks of an Airflow run.
Raw payloads never enter XCom; only small summaries/checksums are returned.
Gold warnings disclose source limitations; any failed quality rule stops the DAG.
"""

import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

try:
    from airflow.sdk import DAG
except ImportError:
    from airflow import DAG
try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
DAG_ID = 'omnichannel_retail_pipeline'
LOG = logging.getLogger(__name__)


def pipeline_id(context):
    return str(uuid5(NAMESPACE_URL, f"aien/{DAG_ID}/{context['run_id']}"))


def encode(value):
    return json.dumps(value, ensure_ascii=False, default=str, allow_nan=False)


def ensure_run(context):
    from shared.db import connect
    run_id = pipeline_id(context)
    with connect('local') as connection:
        connection.execute('''INSERT INTO ops.pipeline_runs
            (pipeline_run_id,pipeline_name,status,metadata)
            VALUES(%s,'omnichannel_retail_airflow','RUNNING',%s::jsonb)
            ON CONFLICT(pipeline_run_id) DO UPDATE SET status='RUNNING',
                finished_at=NULL,error_message=NULL,metadata=ops.pipeline_runs.metadata || EXCLUDED.metadata
        ''', (run_id, encode({'airflow': {'dag_id': DAG_ID, 'run_id': context['run_id']}})))
    LOG.info('Shared pipeline_run_id: %s', run_id)
    return run_id


def record_failure(context):
    """Called after a task exhausts retries, including raw validation failures."""
    try:
        from shared.db import connect
        run_id = pipeline_id(context)
        task = context.get('task_instance') or context.get('ti')
        task_id = getattr(task, 'task_id', 'unknown')
        message = f"{task_id}: {context.get('exception', 'task failed')}"
        with connect('local') as connection:
            connection.execute((PROJECT_ROOT/'database/schemas/ops.sql').read_text(encoding='utf-8-sig'))
        with connect('local') as connection:
            connection.execute('''INSERT INTO ops.pipeline_runs
                (pipeline_run_id,pipeline_name,status,finished_at,error_message,metadata)
                VALUES(%s,'omnichannel_retail_airflow','FAILED',CURRENT_TIMESTAMP,%s,%s::jsonb)
                ON CONFLICT(pipeline_run_id) DO UPDATE SET status='FAILED',
                    finished_at=CURRENT_TIMESTAMP,error_message=EXCLUDED.error_message,
                    metadata=ops.pipeline_runs.metadata || EXCLUDED.metadata
            ''', (run_id,message,encode({'airflow': {'dag_id': DAG_ID,'run_id': context['run_id'],
                                                    'failed_task': task_id}})))
            connection.execute('''INSERT INTO ops.quality_results
                (pipeline_run_id,stage,check_name,status,failed_record_count,details)
                VALUES(%s,'airflow',%s,'FAIL',1,%s::jsonb)
                ON CONFLICT(pipeline_run_id,stage,check_name) DO UPDATE SET
                    status='FAIL',details=EXCLUDED.details,checked_at=CURRENT_TIMESTAMP
            ''',(run_id,f'task:{task_id}',encode({'error':message})))
    except Exception:
        # Airflow retains the original task exception even when PostgreSQL is unavailable.
        LOG.exception('Could not persist failure callback to the retail Ops database')


def validate_raw_files(**context):
    from pipelines.bronze.build_bronze import validate_raw
    sources = validate_raw(PROJECT_ROOT/'data/raw')
    summary = {'file_count':len(sources),'record_count':sum(len(s['records']) for s in sources),
               'input_files': [{'source_file':s['source_file'],'file_sha256':s['file_sha256'],
                                'record_count':len(s['records'])} for s in sources]}
    LOG.info('Raw validation: files=%s records=%s pipeline_run_id=%s',
             summary['file_count'],summary['record_count'],pipeline_id(context))
    return summary


def initialize_schemas(**context):
    from shared.db import connect
    for filename in ('ops.sql','bronze.sql','silver.sql','gold.sql'):
        with connect('local') as connection:
            connection.execute((PROJECT_ROOT/'database/schemas'/filename).read_text(encoding='utf-8-sig'))
    return {'pipeline_run_id':ensure_run(context)}


def load_bronze(**context):
    from pipelines.bronze.build_bronze import build_bronze
    run_id = ensure_run(context)
    result = build_bronze(run_id)
    initial = context['ti'].xcom_pull(task_ids='validate_raw_files')
    expected = {r['source_file']:(r['file_sha256'],r['record_count']) for r in initial['input_files']}
    actual = {r['source_file']:(r['file_sha256'],r['record_count']) for r in result['input_files']}
    if expected != actual:
        raise ValueError('Raw files changed between validation and ingestion; rerun the complete DAG')
    return {key:result[key] for key in ('file_count','record_count','inserted_records')}


def build_silver(**context):
    from pipelines.silver.build_silver import build_silver as build
    run_id = ensure_run(context)
    result = build(run_id,bronze_run_id=run_id)
    return {key:result[key] for key in ('accepted_records','rejected_records','quality_checks_passed')}


def quality_gate(**context):
    from pipelines.gold.build_gold import silver_quality_gate
    run_id = ensure_run(context)
    silver_quality_gate(run_id)
    return {'pipeline_run_id':run_id,'silver_gate':'PASS'}


def build_gold(**context):
    from pipelines.gold.build_gold import build_gold as build
    run_id = ensure_run(context)
    result = build(run_id,silver_run_id=run_id)
    return {'row_counts':result['row_counts'],'quality':result['quality']}


def validate_gold_and_record_metadata(**context):
    from shared.db import connect
    from pipelines.gold.build_gold import (
        GOLD_TYPES,build_rows,read_silver,gold_quality,save_checks,silver_quality_gate,
    )
    from pipelines.silver.build_silver import SPECS
    run_id = ensure_run(context)
    with connect('local') as connection:
        connection.execute('LOCK TABLE '+', '.join('silver.'+n for n in SPECS)+' IN SHARE MODE')
        connection.execute('LOCK TABLE '+', '.join('gold.'+n for n in GOLD_TYPES)+' IN SHARE MODE')
        silver_quality_gate(run_id,connection)
        expected,diagnostics = build_rows(read_silver(connection),run_id)
        checks = gold_quality(connection,run_id,expected,diagnostics)
        failures = [name for name,status,failed,details in checks if status=='FAIL']
        if failures:
            raise ValueError('Final Gold validation failed: '+', '.join(failures))
        save_checks(connection,run_id,checks)
        # A previously failed task can be cleared and recovered in the same DAG run.
        # Record that recovery without claiming the earlier failure never happened.
        connection.execute("""UPDATE ops.quality_results SET status='PASS',failed_record_count=0,
            details=details || '{"recovered_by":"final_gold_validation"}'::jsonb,
            checked_at=CURRENT_TIMESTAMP WHERE pipeline_run_id=%s AND stage='airflow' AND status='FAIL'""",
            (run_id,))
        totals = {'pass':sum(c[1]=='PASS' for c in checks),'warn':sum(c[1]=='WARN' for c in checks),
                  'fail':0,'row_counts':{name:len(rows) for name,rows in expected.items()}}
        connection.execute('''UPDATE ops.pipeline_runs SET status='SUCCESS',
            finished_at=CURRENT_TIMESTAMP,error_message=NULL,metadata=metadata || %s::jsonb
            WHERE pipeline_run_id=%s''',(encode({'airflow_validation':totals}),run_id))
    LOG.info('Complete pipeline SUCCESS: %s | %s',run_id,encode(totals))
    return {'pipeline_run_id':run_id,**totals}


with DAG(
    dag_id=DAG_ID,
    description='Raw -> Bronze -> Silver -> quality gate -> five fixed Gold tables',
    start_date=datetime(2026,1,1,tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={'owner':'Arien','retries':1,'retry_delay':timedelta(seconds=20),
                  'execution_timeout':timedelta(minutes=20),'on_failure_callback':record_failure},
    tags=['AIEN','milestone','retail'],
) as dag:
    raw = PythonOperator(task_id='validate_raw_files',python_callable=validate_raw_files)
    schemas = PythonOperator(task_id='initialize_postgres_schemas',python_callable=initialize_schemas)
    bronze = PythonOperator(task_id='load_bronze',python_callable=load_bronze)
    silver = PythonOperator(task_id='build_silver',python_callable=build_silver)
    gate = PythonOperator(task_id='quality_gate',python_callable=quality_gate)
    gold = PythonOperator(task_id='build_gold',python_callable=build_gold)
    final = PythonOperator(task_id='validate_gold_and_record_metadata',python_callable=validate_gold_and_record_metadata)
    raw >> schemas >> bronze >> silver >> gate >> gold >> final
