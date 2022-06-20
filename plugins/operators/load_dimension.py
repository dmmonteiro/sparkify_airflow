from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Load data into dimension tables in Redshift
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 db_conn_id,
                 db_table,
                 mode,
                 sql,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.db_table = db_table
        self.mode = mode
        self.sql = sql

    def execute(self, context):
        accepted_modes = ['append-only', 'delete-load']
        if self.mode not in accepted_modes:
            raise ValueError(
                f'Mode parameter should be one of these: {accepted_modes}')

        self.log.info(
            f'LoadDimensionOperator: Loading {self.db_table} data...')

        db_hook = PostgresHook(postgres_conn_id=self.conn_id)

        sql = []

        if self.mode == 'delete-load':
            sql.append(f'TRUNCATE TABLE public.{self.db_table};')

        sql.append(f'INSERT INTO public.{self.db_table}({self.sql});')

        db_hook.run(' '.join(sql))

        self.log.info(f'LoadDimensionOperator: {self.db_table} data loaded!')
