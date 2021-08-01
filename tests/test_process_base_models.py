import os
from dbt_generator.process_base_models import ProcessBaseQuery


COLUMNS = ['canmanageclients', 'currencycode', 'customerid', 'datetimezone', 'name', 'testaccount', '_sdc_batched_at',
           '_sdc_customer_id', '_sdc_extracted_at', '_sdc_received_at', '_sdc_sequence', '_sdc_table_version']


def test_ProcessBaseQuery():
    pbq = ProcessBaseQuery(
        sql_file=os.path.join(os.path.dirname(__file__),
                              'test_data', 'test_sql_file.sql'),
        transforms_file=os.path.join(os.path.dirname(
            __file__), 'test_data', 'test_transform.yml')
    )

    assert pbq.columns == COLUMNS
