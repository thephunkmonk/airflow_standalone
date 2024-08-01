from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
	PythonOperator, 
	PythonVirtualenvOperator,
	BranchPythonOperator
)
import pprint

with DAG(
    'movie_summary',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='movie DAG',
    schedule="10 4 * * * ",
#    schedule=timedelta(days=1),
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['api', 'movies'],
) as dag:
	REQS='git+https://github.com/thephunkmonk/movie_dag.git@0.2/api'
	def gen_empty(id):
		return EmptyOperator(task_id=id)

	def gen_vpython(id):
		task = PythonVirtualenvOperator(
			task_id=id,
			python_callable=dummy,
			system_site_packages=False,
			requirements=REQS,
			op_kwargs={
				'url_param': {'multiMovieYn': 'Y'},
				'abc' : {'def' : 'N'}
			}
		)
		return task

	def gen_vpython2(**kw):
		task = PythonVirtualenvOperator(
			task_id=kw['id'],
			python_callable=kw['fun_obj'],
			system_site_packages=False,
			requirements=REQS,
			op_kwargs= kw['op_kwargs']
		)
		return task

	def pro_dum(**params):
		print('@'*33)
		print(params['task_name'])
		print('@'*33)

	def dummy(ds_nodash, **params):
		print(f"{ds_nodash}")
		print(f"{params}")
		print(f"{params['url_param']}")
		print(f"{params['abc']}, {type(params['abc'])}")
		print('dummy')

	start = gen_empty('start')
	end = gen_empty( 'end')
	
	#apply_type = gen_vpython('apply.type')
	apply_type = gen_vpython2(id='apply.type', fun_obj=dummy, 
			op_kwargs={
                                'url_param': {'multiMovieYn': 'Y'},
                                'abc' : {'hey' : 'N'},
				'task_name' : 'apppppppllyyyyyy!!!!'
                        })
	
	merge_df = gen_vpython('merge.df')

	de_dup = gen_vpython('de.dup')
	
	sum_df = gen_vpython('summary.df')

#	start = gen_empty('start')
#	end = gen_empty('end')

	start >> apply_type >> merge_df >> de_dup >> sum_df >> end
