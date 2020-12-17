from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """ 
        Loads data into stagging tables in AWS Redshift
    """

    ui_color = '#358140'
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id = "",
                 table ="",
                 s3_bucket="",
                 s3_key = "",
                 region = "us-west-2",
                 path = "auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params 
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.path = path
        
        

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Copying data from S3 to AWS Redshift")
        s3_generated_key = self.s3_key.format(**context)
        self.s3_path = "s3://{}/{}".format(self.s3_bucket, s3_generated_key)
        

        sql = f"""
        COPY {self.table}
        FROM '{self.s3_path}'
        ACCESS_KEY_ID '{credentials.access_key}'
        SECRET_ACCESS_KEY '{credentials.secret_key}'
        JSON '{self.path}'
        REGION '{self.region}'
        """

        redshift.run(sql)
        





