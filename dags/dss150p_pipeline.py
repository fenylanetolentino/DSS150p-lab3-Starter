from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

PROJECT = '/opt/airflow/project'


def failure_callback(context):
    """Print task, run and error context when a task fails."""
    ti = context['task_instance']
    error = context.get('exception')
    print(
        f"CRITICAL FAILURE: DAG '{ti.dag_id}' | Task '{ti.task_id}' | "
        f"Run ID: {ti.run_id} | Try: {ti.try_number} | Error: {error}"
    )


DEFAULT_ARGS = {
    'owner': 'dss150p',
    # Retries: transient failures (e.g. DB not ready) get 2 more attempts,
    # 1 minute apart, before the task is marked failed.
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    # Timeout: a task that runs longer than 10 minutes is killed, so nothing hangs forever.
    'execution_timeout': timedelta(minutes=10),
    'on_failure_callback': failure_callback,
}

with DAG(
    dag_id='dss150p_sales_pipeline',
    start_date=datetime(2026, 1, 1),
    # Schedule: daily at 02:00. Sales data for the previous day is complete by then,
    # and it runs during off-peak hours, so it does not compete with daytime usage.
    schedule='0 2 * * *',
    # Catch-up: disabled. The pipeline processes the current data snapshot, so
    # replaying every missed day since start_date would repeat identical work.
    # Specific periods are backfilled on purpose via run_mode=partition.
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        'run_mode': Param('full', enum=['full', 'partition']),
        'year': Param(2026, type='integer'),
        'month': Param(1, type='integer', minimum=1, maximum=12),
    },
    tags=['DSS150P', 'modular'],
) as dag:
    # Run identity: the same Airflow run_id is passed to every task as PIPELINE_RUN_ID.
    # Business logic lives in src/, the DAG only calls the CLI entry points.
    extract = BashOperator(
        task_id='extract',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli extract',
    )

    transform = BashOperator(
        task_id='transform',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli transform',
    )

    # export (not a VAR=value prefix) so the variable applies to the whole if/else block
    load = BashOperator(
        task_id='load',
        bash_command=(
            f"cd {PROJECT} && "
            "export PIPELINE_RUN_ID='{{ run_id }}' && "
            "if [ \"{{ params.run_mode }}\" = \"partition\" ]; then "
            "python -m src.cli load-partition --year {{ params.year }} --month {{ params.month }}; "
            "else python -m src.cli load; fi"
        ),
    )

    validate = BashOperator(
        task_id='validate',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli validate',
    )

    # Dependencies: strict sequential order
    extract >> transform >> load >> validate