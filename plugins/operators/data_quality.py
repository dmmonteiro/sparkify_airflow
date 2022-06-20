from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Check data quality of all tables populated by the loading process
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 db_conn_id,
                 dq_checks,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.db_conn_id)
        error_count = 0
        failed_tests = []

        self.log.info('Starting Data Quality checks...')

        for check in self.dq_checks:
            sql = check.get('sql')
            expected_result = check.get('result')

            records = redshift.get_records(sql)[0]

            if expected_result != records[0]:
                error_count += 1
                failed_tests.append(sql)

        if error_count > 0:
            self.log.info(f'Failed tests: {failed_tests}')
            raise ValueError('Data Quality check failed!')

        self.log.info('Data Quality checks succeeded!')
