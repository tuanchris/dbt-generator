import os
import pytest
from unittest.mock import patch
import filecmp
from pathlib import Path
import dbt_generator.process_base_models as dbt_gen


TEST_DATA_DIR = Path(__file__).resolve().parent / 'test_data'
COLUMNS = ['canmanageclients', 'currencycode', 'customerid', 'datetimezone', 'name', 'testaccount',
           '_sdc_batched_at', '_sdc_customer_id', '_sdc_extracted_at', '_sdc_received_at',
           '_sdc_sequence', '_sdc_table_version'
           ]
INPUT_FILE = os.path.join(TEST_DATA_DIR, 'test_sql_file.sql')


def test_get_columns():
    processor = dbt_gen.ProcessBaseModels(INPUT_FILE)
    processor.get_columns()
    assert processor.columns == COLUMNS


def test_get_columns__drop_metadata():
    processor = dbt_gen.ProcessBaseModels(INPUT_FILE, drop_metadata=True)
    processor.get_columns()
    columns = [column for column in COLUMNS if not column.startswith('_')]
    assert processor.columns == columns


def test_invalid_sql_files():
    processor = dbt_gen.ProcessBaseModels(INPUT_FILE, drop_metadata=True)
    processor.lines = []
    with pytest.raises(Exception):
        processor.get_columns()


def test_no_columns_in_sql_files():
    with pytest.raises(Exception):
        dbt_gen.ProcessBaseModels(os.path.join(
            TEST_DATA_DIR, 'test_invalid_sql_file.sql'), drop_metadata=True)


def test_transform__drop_metadata():
    trfm_file = os.path.join(TEST_DATA_DIR, 'test_transform.yml')
    output_file = os.path.join(
        TEST_DATA_DIR, 'out/transformed__drop_metadata.sql')
    expected_file = os.path.join(
        TEST_DATA_DIR, 'expected/transformed__drop_metadata.sql')

    pbq = dbt_gen.ProcessBaseModelsWithTransforms(
        sql_file=INPUT_FILE,
        transforms_file=trfm_file,
        drop_metadata=True
    )
    pbq.process_base_models(output_file)
    assert filecmp.cmp(expected_file, output_file, shallow=False)


def test_transform__keep_metadata():
    trfm_file = os.path.join(TEST_DATA_DIR, 'test_transform.yml')
    output_file = os.path.join(
        TEST_DATA_DIR, 'out/transformed__keep_metadata.sql')
    expected_file = os.path.join(
        TEST_DATA_DIR, 'expected/transformed__keep_metadata.sql')

    pbq = dbt_gen.ProcessBaseModelsWithTransforms(
        sql_file=INPUT_FILE,
        transforms_file=trfm_file,
        drop_metadata=False
    )

    pbq.process_base_models(output_file)

    assert filecmp.cmp(expected_file, output_file, shallow=False)


def test_ProcessBaseModelsBQ():
    output_file = os.path.join(
        TEST_DATA_DIR, 'out/ProcessBaseModelsBQ.sql')
    expected_file = os.path.join(
        TEST_DATA_DIR, 'expected/ProcessBaseModelsBQ.sql')
    processor = dbt_gen.ProcessBaseModelsBQ(
        sql_file=os.path.join(TEST_DATA_DIR, 'test_sql_file.sql'),
        split_columns=True, convert_timestamp=True, id_as_int=True
    )
    processor.process_base_models(output_file)
    assert filecmp.cmp(expected_file, output_file, shallow=False)


def test_ProcessBaseModelsSF():
    output_file = os.path.join(
        TEST_DATA_DIR, 'out/ProcessBaseModelsSF.sql')
    expected_file = os.path.join(
        TEST_DATA_DIR, 'expected/ProcessBaseModelsSF.sql')
    processor = dbt_gen.ProcessBaseModelsSF(
        sql_file=os.path.join(TEST_DATA_DIR, 'test_sql_file.sql'),
        split_columns=True, convert_timestamp=True, id_as_int=True
    )
    processor.process_base_models(output_file)
    assert filecmp.cmp(expected_file, output_file, shallow=False)
