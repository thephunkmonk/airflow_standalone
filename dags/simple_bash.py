from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
with DAG(
    'simple-bash',
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
    schedule="10 4 * * * ",
 #   schedule=timedelta(days=1),
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['simple', 'bash','etl','shop'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    task_date = BashOperator(
        task_id='print_date',
        bash_command="""
             echo "date => `date`"
            
             echo "logical_date => {{logical_date}}"
             echo "logical_date => {{logical_date.strftime("%Y-%m-%d %H:%M:%S")}}"

             echo "execution_date => {{execution_date}}"
             echo "execution_date => {{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}"
             echo "next_execution_date => {{next_execution_date.strftime("%Y-%m-%d %H:%M:%S")}}"
             echo "prev_execution_date => {{prev_execution_date.strftime("%Y-%m-%d %H:%M:%S")}}"
             
             echo "ts => {{ts}}"
             echo "ds => {{ds}}"
             echo "ds_nodash => {{ds_nodash}}"
        """
    )

    task_copy = BashOperator(
        task_id="copy.log",
        bash_command="""
               mkdir -p ~/data/{{ds_nodash}} 
               cp ~/app/savehistory/log/history_{{ds_nodash}}*.log ~/data/{{ds_nodash}}/
        """
    )
    
    task_cut = BashOperator(
        task_id="cut.log",
        bash_command="""
             echo "cut"
             mkdir -p ~/data/cut/{{ds_nodash}}
             cat ~/data/{{ds_nodash}}/* | cut -d' ' -f1 > ~/data/{{ds_nodash}}/cut.log
    """,
        trigger_rule="none_failed"
    )

    task_sort = BashOperator(
        task_id="sort.log",
        bash_command="""
           echo 'sort'
           mkdir -p ~/data/sort/{{ds_nodash}}
           cat ~/data/{{ds_nodash}}/cut.log | sort > ~/data/{{ds_nodash}}/sort.log
    """

    )

    task_count = BashOperator(
        task_id="count.log",
        bash_command="""
            echo 'count'
            mkdir -p ~/data/count/{{ds_nodash}}
            cat ~/data/{{ds_nodash}}/sort.log | uniq -c > ~/data/{{ds_nodash}}/count.log
    """
    )

    task_err = BashOperator(
        task_id="err.report",
        bash_command="""
              echo "error"
    """,
        trigger_rule="one_failed"
    )
    
    task_done= BashOperator(
        task_id="make.done",
        bash_command="""
            DONE_PATH=~/data/done/{{ds_nodash}}
            mkdir -p ${DONE_PATH} 
            touch ${DONE_PATH}/_DONE
    """   
)  

    task_start = EmptyOperator(task_id ='task_start')
    task_end = EmptyOperator(task_id = 'task_end', trigger_rule="all_done")

    task_start >> task_date >> task_copy >> task_cut >> task_sort >> task_count >> task_done >>  task_end
    task_copy >> task_err >> task_end
