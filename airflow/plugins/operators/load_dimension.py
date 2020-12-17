from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'        
        
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 delete=False,
                 sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.delete = delete
        self.sql = sql

    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete:
            redshift.run(f"DELETE FROM {self.table}")

        formatted_sql = f"""INSERT INTO {self.table} ({self.sql});"""

        redshift.run(formatted_sql)