from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
with DAG(
    'import_db',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='simple bash DAG',
 #   schedule="10 4 * * * ",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 7, 11),
    catchup=True,
    tags=['import', 'bash','etl','db'],
) as dag:

    task_check = BashOperator(
        task_id="check",
        bash_command="""
        bash {{ var.value.check_sh_path }} {{ds_nodash}}
    """
    )
    
    task_csv = BashOperator(
        task_id="to_csv",
        bash_command="""
          CSV_PATH=~/data/csv/{{ds_nodash}}
          COUNT_PATH=~/data/{{ds_nodash}}
          
          mkdir -p $CSV_PATH
          cat $COUNT_PATH/count.log | awk '{print "{{ds}},"$2 "," $1}' > ~/data/csv/{{ds_nodash}}/csv.csv
    """,
        trigger_rule="none_failed"
    )

    task_tmp = BashOperator(
        task_id="to_tmp",
        bash_command="""
           echo 'to tmp'
    """

    )

    task_base = BashOperator(
        task_id="to_base",
        bash_command="""
           echo "to base"
    """
    )

    task_done= BashOperator(
        task_id="make.done",
        bash_command="""
           echo "donezo"
    """   
    )

    task_err = BashOperator(
        task_id="err.report",
        bash_command="""
              echo "error"
    """,
        trigger_rule="one_failed"
    )

    task_start = EmptyOperator(task_id ='task_start')
    task_end = EmptyOperator(task_id = 'task_end', trigger_rule="all_done")

    task_start >> task_check >> task_csv >> task_tmp >> task_base >> task_done >> task_end
    task_check >> task_err >> task_end
