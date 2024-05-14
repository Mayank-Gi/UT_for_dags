import unittest
import re
import importlib.util
import json
from pylint.lint import Run


def get_tasks_from_dag_file(dag_file):
    with open(dag_file, 'r') as file:
        content = file.read()
    tasks = re.findall(r"task_id='(.*?)'", content)
    return tasks


def get_dag_properties_from_dag_file(dag_file):
    with open(dag_file, 'r') as file:
        content = file.read()
    dag_id = re.search(r"DAG_NAME = '(.*?)'", content)
    default_args = re.search(r"default_args = (.*?)\n", content)
    schedule_interval = re.search(r"schedule_interval=(.*?),", content)
    return dag_id.group(1) if dag_id else None, default_args.group(
        1) if default_args else None, schedule_interval.group(1) if schedule_interval else None


class TESTDAG(unittest.TestCase):
    def setUp(self):
        with open("C:/Users/offic/PycharmProjects/testing_n_poc/new/cvm_fsn_location_config.json", "r") as f:
            self.config = json.load(f)

    def test_task_count(self):
        """Check task count of TESTDAG dag"""
        tasks = get_tasks_from_dag_file('C:/Users/offic/PycharmProjects/testing_n_poc/new/t2.py')
        self.assertEqual(first=len(tasks), second=7)

    def test_contain_tasks(self):
        """Check task contains in TESTDAG dag"""
        task_ids = get_tasks_from_dag_file('C:/Users/offic/PycharmProjects/testing_n_poc/new/t2.py')
        self.assertListEqual(list1=task_ids,
                             list2=['Start', 'end', 'export_to_s3', 'get_increment_dates', 'dag_audit', 'dag_audit_old',
                                    'email'])

    # def test_import_dag_file(self):
    #     '''loading .py fi;e'''
    #     spec = importlib.util.spec_from_file_location("module.name",
    #                                                   ")
    #     module = importlib.util.module_from_spec(spec)
    #     try:
    #         spec.loader.exec_module(module)
    #     except Exception as e:
    #         assert False, f"DAG import failed: {e}"

    def test_dag_properties(self):
        """Check start_date, end_date, and catchup of TESTDAG dag"""
        dag_id, default_args, schedule_interval = get_dag_properties_from_dag_file("C:/Users/offic/PycharmProjects"
                                                                                   "/testing_n_poc/new/t2.py")
        self.assertEqual(first=dag_id, second='cvm_fsn_location')

    def test_dag_owner(self):
        self.assertEqual(first=self.config['cvm_fsn_location_config']['default_args']['owner'], second='CVM_TEAM')

    def test_dag_schedule_interval_and_start_date(self):
        self.assertEqual(first=self.config['cvm_fsn_location_config']['default_args']['schedule_interval'],
                         second="0 5 * * *")
        self.assertIsNotNone(self.config['cvm_fsn_location_config']['default_args']['schedule_interval'])
        self.assertIsNotNone(self.config['cvm_fsn_location_config']['default_args']['start_date'])

    def test_dag_sql_path(self):
        self.assertEqual(first=self.config['cvm_fsn_location_config']['default_args']['params']['sql_path'],
                         second='fsn_location.sql')

    def test_dag_audit_src_tbl_count(self):
        self.assertEqual(
            len(self.config['cvm_fsn_location_config']['default_args']['params']['dag_audit_src_tbl_count_sql']), 2)

    def test_pylint_score(self):
        results = Run(['--load-plugins=pylint_airflow', 'C:/Users/offic/PycharmProjects/testing_n_poc/new/t2.py'],
                      do_exit=False)
        # `exit` is deprecated, use `do_exit` instead
        self.assertGreaterEqual(results.linter.stats['global_note'], 9)

    def test_dag_connection_ids(self):
        self.assertEqual(
            self.config['cvm_fsn_location_config']['default_args']['params']['vertica_conn_id'], 'vertica_etl')
        self.assertEqual(
            self.config['cvm_fsn_location_config']['default_args']['params']['s3_conn_id'], 'cvm_prd_s3')
        self.assertEqual(
            self.config['cvm_fsn_location_config']['default_args']['params']['s3_bucket'], 'cvm-prod-landing-5d6c06b')


if __name__ == '__main__':
    unittest.main()
