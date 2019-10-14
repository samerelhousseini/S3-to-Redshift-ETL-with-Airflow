from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 sql_tests=[],
                 expected_results = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.sql_tests = sql_tests
        self.expected_results = expected_results

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if len(self.sql_tests) != len(self.expected_results):
            raise ValueError('Tests and expected results do not match in lengths')


        for i in range(len(self.sql_tests)):
            res = redshift.get_first(self.sql_tests[i])
            if res[0] != self.expected_results[i]:
                raise ValueError('Test {} failed'.format(i))
            else:
                self.log.info("Test {} passed".format(i))


        