from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    Load data into fact table in Redshift
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 db_conn_id,
                 db_table,
                 sql,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.db_table = db_table
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.db_conn_id)
        self.log.info('LoadFactOperator: Loading fact table data...')

        sql = f'''
            INSERT INTO {self.db_table}({self.sql});
            COMMIT;
        '''
        redshift.run(sql)

        self.log.info('LoadFactOperator: fact table data loaded!')
