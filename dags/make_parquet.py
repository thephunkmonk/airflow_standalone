from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

def  gen_emp(id, rule='all_success'):
	op = EmptyOperator(task_id=id, trigger_rule=rule)
	return op

with DAG(
    'make_parquet',
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
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['import', 'bash','etl','db'],
) as dag:
   task_start = gen_emp('start')
   task_end = gen_emp('end','all_done')
#EmptyOperator(task_id = 'task_end', trigger_rule="all_done")
   task_check = BashOperator(
        task_id="check",
        bash_command="""
        DONE_FILE={{ var.value.IMPORT_DONE_PATH }}/{{ds_nodash}}/_DONE
        bash {{ var.value.check_sh_path }} $DONE_FILE
    """
   )
   task_parq = BashOperator(
	task_id = 'to.parquet',
	bash_command="""
		echo "to.parquet"

		READ_PATH=~/data/csv/{{ds_nodash}}/csv.csv
		SAVE_PATH=~/data/parquets/{{ds_nodash}}
		
		mkdir -p $SAVE_PATH 
		
		python ~/airflow/pys/csv2parquet.py $READ_PATH $SAVE_PATH
	"""
   )
   
   task_done= BashOperator(
        task_id="make.done",
        bash_command="""
            figlet "make.done.start"

            DONE_PATH=~/data/done/import/{{ds_nodash}}
            mkdir -p ${DONE_PATH}
            touch ${DONE_PATH}/_DONE
            
            # mkdir -p {{ var.value.IMPORT_DONE_PATH }}/{{ds}}
            # {{ var.value.IMPORT_DONE_PATH }}/_DONE
            
            figlet "make.done.end"
    """
   )
   task_err = BashOperator(
        task_id="err.report",
        bash_command="""
            echo "error"
        """,
        trigger_rule="one_failed"
   ) 
task_start >> task_check >> task_parq >> task_done >> task_end
task_check >> task_err >> task_end
