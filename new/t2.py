import datetime
import logging
import os
import json
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from operators.vsql_operator import VSQLOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.models import TaskInstance
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator

DAG_NAME = 'cvm_fsn_location'

# Static code
DAG_CONFIG = Variable.get(DAG_NAME + '_config',
                          deserialize_json=True,
                          default_var={})
SCHEDULE = DAG_CONFIG.get('schedule_interval','0 5 * * *')
MAX_ACTIVE_RUNS = DAG_CONFIG.get('max_active_runs', 1)
ARGS = DAG_CONFIG.get('default_args', {})
START_DATE = datetime.datetime.strptime(ARGS.get('start_date', '2023-01-26'),'%Y-%m-%d')
CATCHUP = DAG_CONFIG.get('catchup', False)
ARGS['start_date'] = START_DATE
params = ARGS.get('params')

s3_conn = BaseHook.get_connection(params['s3_conn_id'])
params['aws_id'] = s3_conn.login
params['aws_secret'] = s3_conn.password
params['region'] = json.loads(s3_conn.extra).get("region", 'eu-west-1')
JOB_ROLE_ARN = params['JOB_ROLE_ARN']
current_datetime = datetime.datetime.now()
current_datetime_str = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
log_detail = current_datetime_str.replace(" ", "_").replace(":", "_")
S3_LOGS_BUCKET = params["S3_LOGS_BUCKET"] + f"/{log_detail}"
vertica_conn = BaseHook.get_connection(params['vertica_conn_id'])
vertica_userid = vertica_conn.login
vertica_password = vertica_conn.password
vertica_jdbc_url = params["jdbc_url"]
jar_file = params['jar_file_path']

with open(jar_file, 'r') as file:
    html_content = file.readlines()
jars = ",".join(html_content).replace("\n", '')
stg_schema = params['stg_schema']
table_name = params['write_table_name']
s3_bucket = params['s3_bucket']
s3_location = params['s3_location']

data_string = json.dumps(params)
DEFAULT_MONITORING_CONFIG = {
    "monitoringConfiguration": {
        "s3MonitoringConfiguration": {"logUri": f"{S3_LOGS_BUCKET}"}
    },
}

dag = DAG(
    dag_id=DAG_NAME,
    default_args=ARGS,
    schedule_interval=SCHEDULE,
    max_active_runs=MAX_ACTIVE_RUNS,
    catchup=False
)
start = BashOperator(
    task_id='Start',
    do_xcom_push=True,
    bash_command="echo $(date '+%Y-%m-%d %H:%M:%S %p')",
    dag=dag
)

end = EmptyOperator(
    task_id='end',
    dag=dag,
)

sql_path = 'assets/{}'.format(params["sql_path"])
export_to_s3 = VSQLOperator(
    task_id='export_to_s3',
    vertica_conn_id=params["vertica_conn_id"],
    sql=sql_path,
    dag=dag
)

def get_count_old(**kwargs):
    ti = kwargs['ti']
    connection = BaseHook.get_connection(params['vertica_conn_id'])
    password = connection.password
    username = connection.login
    host_url = connection.host
    port = connection.port
    v_db_name = 'CA_Dev'
    from vertica_python import connect
    vertica_conn_info = {'host': host_url, 'port': port, 'user': username, 'password': password, 'database': v_db_name}
    connection_vertica = connect(**vertica_conn_info)
    cursor_vertica = connection_vertica.cursor()

    parent_tasks = kwargs['task'].upstream_task_ids
    logging.info(parent_tasks)
    for i in list(parent_tasks):
        i="export_to_s3"
        task = kwargs['dag'].get_task(i)
        logging.info(task)
        task_instance = TaskInstance(task, kwargs['execution_date'])
        current_status = task_instance.current_state()
    if current_status.lower() == 'success':
        status = 'success'
        ti.xcom_push('status', status)
    else:
        status = 'failed'
        ti.xcom_push('status', status)

    import time
    epoch_time = int(time.time())

    for i in range(len(params['dag_audit_src_tbl_count_sql'])):
        cursor_vertica.execute(params['dag_audit_src_tbl_count_sql'][i])
        rows_vertica = cursor_vertica.fetchall()
        for x in rows_vertica:
            print("query,count :", params['dag_audit_src_tbl_count_sql'][i], x)
            insert_query = "insert into {} values ('{}','{}','{}',{},{}, '{}',{})" \
                .format(params['dag_audit_table_old'],DAG_NAME,params['dag_audit_src_tbl'][i],params['dag_audit_table_type'][i],params['load_date_range'][i],x[0],status,epoch_time)
            cursor_vertica.execute(insert_query)
            connection_vertica.commit()

        dag_audit_info = "select * from {} where dag_name = '{}' and epoch_time = {} and table_name = '{}'" \
            .format(params['dag_audit_table_old'],DAG_NAME,epoch_time,params['dag_audit_src_tbl'][i])
        cursor_vertica.execute(dag_audit_info)
        r = cursor_vertica.fetchall()
        dag_audit_table_cols = ["dag_name","table_name","type","load_date","count","status"]
        for j in range(len(dag_audit_table_cols)):
            ti.xcom_push(params['dag_audit_src_tbl'][i] + '_' + dag_audit_table_cols[j], r[0][j])


def get_dates(**kwargs):
    src_incremental_column = params['src_incremental_column']
    excel_incremental_column = params['excel_incremental_column']
    source_tables = ",".join(params['source_tables'])
    src_table_or_view = params['src_table_or_view']
    connection = BaseHook.get_connection(params['vertica_conn_id'])
    password = connection.password
    username = connection.login
    host_url = connection.host
    port = connection.port
    v_db_name = 'CA_Dev'
    from vertica_python import connect
    vertica_conn_info = {'host': host_url, 'port': port, 'user': username, 'password': password, 'database': v_db_name}
    connection_vertica = connect(**vertica_conn_info)
    cur = connection_vertica.cursor()
    incerment_query = ""
    if params["incerment_query"].lower() != "None".lower():
        incerment_query = params["incerment_query"].lower()
    else:
        incerment_query = f"select distinct {src_incremental_column}::varchar from {src_table_or_view}"
    print(incerment_query)
    cur.execute(incerment_query)
    data = cur.fetchall()
    if data:
        dd = [str(d[0]) for d in data]
        dates = ",".join(dd)
        print("==========", dates)
        kwargs['ti'].xcom_push('dates', dates)
    else:
        kwargs['ti'].xcom_push('dates', "None")
        print("dates....", data)
    cur.close()
    connection_vertica.close()


def get_count(**kwargs):
    ti = kwargs['ti']
    dag_run_date = datetime.datetime.now().strftime("%Y-%m-%d")
    task = kwargs['dag']
    src_incremental_column=params['src_incremental_column']
    excel_incremental_column = params['excel_incremental_column']
    source_tables=",".join(params['source_tables'])
    src_table_or_view=params['src_table_or_view']
    excel_table=params['excel_table']
    ti = kwargs["ti"]
    start_timestamp = ti.xcom_pull(key='return_value')
    parent_tasks = kwargs['task'].upstream_task_ids
    for i in list(parent_tasks):
        task = kwargs['dag'].get_task(i)
        logging.info(task)
        task_instance = TaskInstance(task, kwargs['execution_date'])
        current_status = task_instance.current_state()
    if current_status.lower() == 'success':
        status = 'Succeeded'
        ti.xcom_push('status', status)
    else:
        status = 'failed'
        ti.xcom_push('status', status)
    context = kwargs
    connection = BaseHook.get_connection(params['vertica_conn_id'])
    password = connection.password
    username = connection.login
    host_url = connection.host
    port = connection.port
    v_db_name = 'CA_Dev'
    from vertica_python import connect
    vertica_conn_info = {'host': host_url, 'port': port, 'user': username, 'password': password, 'database': v_db_name}
    connection_vertica = connect(**vertica_conn_info)
    cur= connection_vertica.cursor()
    dates = ti.xcom_pull(key='dates').split(",")
    print("#######dates#######---", dates)
    filter_on_scr_query = params["filter_on_scr_query"]
    filter_on_excel_query = params["filter_on_excel_query"]
    if dates[0] != "None":
        total_query = {}
        count_dict = {}
        for i in dates:
            date = i
            if filter_on_scr_query.lower().strip() != "none":
                src_query = f"select count(*) from {src_table_or_view} where {src_incremental_column}='{date}'  {filter_on_scr_query}"
            elif params['scr_query'].lower()!='none':
                src_query=params['scr_query']
            else:
                src_query = f"select count(*) from {src_table_or_view} where {src_incremental_column}='{date}'"
            if filter_on_excel_query.lower().strip() != "none":
                excel_query = f"select count(*) from {excel_table} where {excel_incremental_column}='{date}' {filter_on_excel_query}"
            elif params['excel_query'].lower() != 'none':
                excel_query = params['excel_query']
            else:
                excel_query = f"select count(*) from {excel_table} where {excel_incremental_column}='{date}'"
            print("src_query",src_query)
            print("excel_query",excel_query)
            total_query[date] = [src_query,excel_query]
        for k, v in total_query.items():
            source_query = v[0]
            e_query = v[1]
            source_query_execute = cur.execute(source_query)
            source_count = source_query_execute.fetchone()[0]
            e_query_execute = cur.execute(e_query)
            excel_count = e_query_execute.fetchone()[0]
            count_dict[k]={'source_count':source_count,'excel_count':excel_count}

        # dag_audit_hash_id>>dag_name>>
        # source_table>>target_table>>status>>start_timestamp>>end_timestamp>>Src_count>>Exsell_count>>TRX Date
        end_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %p")
        for k,v in count_dict.items():
            dag_name = context['dag_run'].dag_id
            dag_start_timestamp = start_timestamp
            dag_end_timestamp = end_timestamp
            dag_source_tables = source_tables
            dag_excel_table = params['excel_table']
            dag_source_count = v['source_count']
            dag_excel_count = v['excel_count']
            dag_trx_date=k
            if status == 'failed':
                dag_excel_count=0

            dag_hash=hash((dag_name,dag_source_tables,dag_excel_table,"success",dag_start_timestamp,dag_end_timestamp,
                           dag_source_count,dag_excel_count,dag_trx_date))
            print(
                f"""IF PART***,dag_hash>>{dag_hash},dag_name>>{dag_name},source_tables>>{dag_source_tables},target_table>>{dag_excel_table},
                                "Status">>{status},dag_start_timestamp>>{dag_start_timestamp},dag_end_timestamp>>{dag_end_timestamp},
                                   source_count>>{dag_source_count},excel_count>>{dag_excel_count},trx_date>>{dag_trx_date}""")

            insert_query = f"insert into {params['dag_audit_table']} values ('{dag_hash}','{dag_name}','{source_tables}','{dag_excel_table}','{status}', '{dag_start_timestamp}','{dag_end_timestamp}','{dag_source_count}','{dag_excel_count}','{dag_trx_date}'::date,'{dag_run_date}'::date)"
            print(insert_query)
            cur.execute(insert_query)
        connection_vertica.commit()
    else:
        dag_name = context['dag_run'].dag_id
        dag_start_timestamp = start_timestamp
        dag_end_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %p")
        dag_source_tables = source_tables
        dag_excel_table = params['excel_table']
        dag_source_count = 0
        dag_excel_count = 0
        dag_trx_date='NULL'
        dag_hash = hash(
            (dag_name, dag_source_tables, dag_excel_table, "success", dag_start_timestamp, dag_end_timestamp,
             dag_source_count, dag_excel_count, dag_trx_date))
        print(
            f"""ELSE PART***,dag_hash>>{dag_hash},dag_name>>{dag_name},source_tables>>{dag_source_tables},target_table>>{dag_excel_table},
                                       "Status">>{status},dag_start_timestamp>>{dag_start_timestamp},dag_end_timestamp>>{dag_end_timestamp},
                                          source_count>>{dag_source_count},excel_count>>{dag_excel_count},trx_date>>{dag_trx_date}""")
        insert_query = f"insert into {params['dag_audit_table']} values ('{dag_hash}','{dag_name}','{source_tables}','{dag_excel_table}','{status}', '{dag_start_timestamp}','{dag_end_timestamp}','{dag_source_count}','{dag_excel_count}',{dag_trx_date}::date,'{dag_run_date}'::date)"
        print(insert_query)
        cur.execute(insert_query)
        connection_vertica.commit()
    x_com_push = list(
        zip(params["dag_audit_src_tbl"], params["dag_audit_table_type"], params['dag_audit_src_tbl_count_sql']))
    dag_audit_table_cols = ["dag_name", "table_name", "type", "load_date", "count", "status"]
    x_com_push_dict = {}
    for data in x_com_push:
        table_nam = data[0]
        type = data[1]
        query = data[2]
        print("Query....",query)
        cur.execute(query)
        count = cur.fetchone()[0]
        print("count..",count)
        if type=='tgt' and status=='failed':
            count=0
            print("Excel_count",count)

        x_com_push_dict[table_nam + "_" + "table_name"] = table_nam
        x_com_push_dict[table_nam + "_" + "count"] = count
        x_com_push_dict[table_nam + "_" + "dag_name"] = dag_name
        x_com_push_dict[table_nam + "_" + "status"] = status
        x_com_push_dict[table_nam + "_" + "type"] = type
        x_com_push_dict[table_nam + "_" + "status"] = status
        x_com_push_dict[table_nam + "_" + "load_date"] = dag_run_date
    print("###x_com_push_dict", x_com_push_dict)

    for k, v in x_com_push_dict.items():
        ti.xcom_push(k, v)

    connection_vertica.close()


dag_incerment_dates = PythonOperator(
    dag=dag,
    task_id='get_increment_dates',
    provide_context=True,
    trigger_rule=TriggerRule.ALL_DONE,
    python_callable=get_dates
)

dag_audit = PythonOperator(
    dag=dag,
    task_id='dag_audit',
    provide_context=True,
    trigger_rule=TriggerRule.ALL_DONE,
    python_callable=get_count
)
dag_audit_old = PythonOperator(
    dag=dag,
    task_id='dag_audit_old',
    provide_context=True,
    trigger_rule=TriggerRule.ALL_DONE,
    python_callable=get_count_old
)
html_content_file_path = os.path.join(
    os.path.dirname(__file__),
    'cvm_fsn_location_auditing_html_content.html'
)

with open(html_content_file_path, 'r') as file:
    html_content = file.read()

email = EmailOperator(
    task_id='email',
    to=params["to_emails"],
    subject="CVM Export - FSN Location",
    html_content=html_content,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag)

write_s3 = EmrServerlessStartJobOperator(
            task_id="write_s3",
            application_id=params['application_id'],
            execution_role_arn=JOB_ROLE_ARN,
            job_driver={
                "sparkSubmit": {
                    "entryPoint": params["entryPoint"],
                    "entryPointArguments": [
                        params['aws_id'], params['aws_secret'], vertica_userid, vertica_password, vertica_jdbc_url,
                     s3_bucket, "{{ execution_date }}", data_string, DAG_NAME
                    ],
                    "sparkSubmitParameters": " ".join([
                        f"--conf spark.executor.instances={params['executor_instances']}",
                        f"--conf spark.executor.memory={params['executor_memory']}",
                        f"--conf spark.executor.cores={params['executor_cores']}",
                        f"--conf spark.driver.cores={params['driver_cores']}",
                        f"--conf spark.driver.memory={params['driver_memory']}",
                        f"--conf spark.dynamicAllocation.enabled={params['dynamic_allocation']}",
                        f"--conf spark.dynamicAllocation.minExecutors={params['min_executors']}",
                        f"--conf spark.dynamicAllocation.maxExecutors={params['max_executors']}",
                        f"--jars {jars}"
                    ])
                }
            },
            configuration_overrides=DEFAULT_MONITORING_CONFIG,
            trigger_rule=TriggerRule.ALL_DONE,
            dag=dag
        )

start >>dag_incerment_dates>> export_to_s3 >> write_s3 >> dag_audit>>email>>dag_audit_old >>end