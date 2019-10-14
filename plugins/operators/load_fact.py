from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table = "",
                 create_table="",
                 load_table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.create_table = create_table
        self.load_table = load_table


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Create if table not exists destination Redshift table")
        redshift.run(self.create_table)

        self.log.info("Loading into destination Redshift table")
        redshift.run("INSERT INTO {} {}".format(self.table, self.load_table))
