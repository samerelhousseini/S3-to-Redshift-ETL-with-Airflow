from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    load_sql = """
        INSERT INTO {}
        {}
    """ 

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table = "",
                 create_table="",
                 load_table="",
                 truncateInsert=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
                
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.create_table = create_table
        self.load_table = load_table
        self.truncateInsert = truncateInsert

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")

        if self.truncateInsert == True:
            redshift.run("DROP TABLE IF EXISTS {}".format(self.table))

        redshift.run(self.create_table)

        self.log.info("Loading into destination Redshift table")
        load_sql_str = LoadDimensionOperator.load_sql.format(self.table, self.load_table)
        redshift.run(load_sql_str)
