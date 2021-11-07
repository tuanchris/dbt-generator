import os

import dbt_generator.process_base_models as dbt_gen
import filecmp

COLUMNS = ['canmanageclients', 'currencycode', 'customerid', 'datetimezone', 'name', 'testaccount', 
          '_sdc_batched_at','_sdc_customer_id', '_sdc_extracted_at', '_sdc_received_at', 
          '_sdc_sequence', '_sdc_table_version'
          ]


def test_ProcessBaseQuery():
    pbq = dbt_gen.ProcessBaseQuery(
        sql_file=os.path.join(os.path.dirname(__file__),
                              'test_data', 'test_sql_file.sql'),
        transforms_file=os.path.join(os.path.dirname(
            __file__), 'test_data', 'test_transform.yml')
    )

    assert pbq.columns == COLUMNS


def test_transform__drop_metadata():
    input_file = os.path.join(os.path.dirname(__file__),'test_data', 'test_sql_file.sql')
    trfm_file = os.path.join(os.path.dirname(__file__), 'test_data', 'test_transform.yml')
    output_file = os.path.join(os.path.dirname(__file__),'test_data/out', 'transformed__drop_metadata.sql')
    expected_file = os.path.join(os.path.dirname(__file__),'test_data/expected', 'transformed__drop_metadata.sql')

    pbq = dbt_gen.ProcessBaseQuery(
        sql_file= input_file,
        transforms_file= trfm_file,
        drop_metadata=True
    )

    pbq.write_file(output_file)

    assert filecmp.cmp(expected_file, output_file, shallow=False)


def test_transform__keep_metadata():
    input_file = os.path.join(os.path.dirname(__file__),'test_data', 'test_sql_file.sql')
    trfm_file = os.path.join(os.path.dirname(__file__), 'test_data', 'test_transform.yml')
    output_file = os.path.join(os.path.dirname(__file__),'test_data/out', 'transformed__keep_metadata.sql')
    expected_file = os.path.join(os.path.dirname(__file__),'test_data/expected', 'transformed__keep_metadata.sql')

    pbq = dbt_gen.ProcessBaseQuery(
        sql_file= input_file,
        transforms_file= trfm_file,
        drop_metadata=False
    )

    pbq.write_file(output_file)

    assert filecmp.cmp(expected_file, output_file, shallow=False)
