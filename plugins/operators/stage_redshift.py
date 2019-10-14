from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook



class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT AS {}
    """ 

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region = "",
                 create_table = "",
                 format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = Variable.get(s3_bucket)
        # execution_date = kwargs["execution_date"]
        # self.s3_key = s3_key.format(execution_date.year,execution_date.month,execution_date.day)
        self.s3_key = s3_key
        
        self.format = format
        self.region = region
        self.create_table = create_table
        self.aws_credentials_id = aws_credentials_id


    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DROP TABLE IF EXISTS {}".format(self.table))
        redshift.run(self.create_table)

        self.log.info("Copying data from S3 to Redshift")

        for c in context:
            self.log.info('Context: ' + str(c))
        
        rendered_key = self.s3_key.format(context['execution_date'])
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.format
        )
        redshift.run(formatted_sql)





