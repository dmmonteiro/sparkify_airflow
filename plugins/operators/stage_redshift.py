from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Load data into staging tables in Redshift
    """

    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 db_conn_id,
                 db_table,
                 aws_conn_id,
                 s3_bucket,
                 s3_key,
                 db_extra,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.db_table = db_table
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.db_extra = db_extra

    def execute(self, context):
        self.log.info(
            f'StageToRedshiftOperator: Loading {self.db_table} data...')

        aws_hook = AwsHook(self.aws_conn_id)
        aws_keys = aws_hook.get_credentials()
        db_hook = PostgresHook(postgres_conn_id=self.db_conn_id)
        s3_path = f's3://{self.s3_bucket}/{self.s3_key}'

        sql = f'''
            COPY {self.db_table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{aws_keys.access_key}'
            SECRET_ACCESS_KEY '{aws_keys.secret_key}'
            region 'us-west-2'
            timeformat 'epochmillisecs'
            {self.db_extra}
        '''

        db_hook.run(sql)

        self.log.info(
            f'StageToRedshiftOperator: {self.db_table} data loaded!')
