from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from pprint import pprint

def gen_emp(id, rule='all_success'):
	op = EmptyOperator(task_id=id, trigger_rule=rule)
	return op

with DAG(
    'movie',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='simple bash DAG',
    schedule="10 4 * * * ",
#    schedule=timedelta(days=1),
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['api', 'movies'],
) as dag:
	def get_data(ds, **kwargs):
		print(ds)
		print(kwargs)
		print("*" * 20)
		print(f"ds_nodash => {kwargs['ds_nodash']}")
		print(f"kwargs type => {type(kwargs)}")
		print("*" * 20)
		from mov.api.call import gen_url, req, get_key, req2list, list2df, save2df
		key = get_key()
		print(f"movie api key => {key}")
		ymd = kwargs['ds_nodash']
		df = save2df(ymd)
		print(df.head(5))

	def print_context(ds=None, **kwargs):
		"""Print the Airflow context and ds variable from the context."""
		print("::group::All kwargs")
		pprint(kwargs)
		print(kwargs)
		print("::endgroup::")
		print("::group::Context variable ds")
		print(ds)
		print("::endgroup::")
		return "Whatever you return gets printed in the logs"

	run_this = PythonOperator(task_id="print_the_context", python_callable=print_context)

	task_start = gen_emp('start')
	task_end = gen_emp('end','all_done')
#EmptyOperator(task_id = 'task_end', trigger_rule="all_done")
	task_get = PythonOperator(
        	task_id="get.data",
		python_callable=get_data
	)
	task_save = BashOperator(
		task_id='save.data',
		bash_command="""
			echo "save.data"
		"""
   	)
   
	task_done = BashOperator(
        	task_id="make.done",
        	bash_command="""
			echo 'done'
	"""
	)
	task_err = BashOperator(
        	task_id="err.report",
        	bash_command="""
			echo "error"
		""",
        	trigger_rule="one_failed"
	) 
task_start >> task_get >> task_save >> task_done >> task_end
task_start >> run_this >> task_end

#task_check >> task_err >> task_end
