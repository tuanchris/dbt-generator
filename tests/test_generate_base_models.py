import os
import sys, os
from dbt_generator.generate_base_models import generate_base_model, get_base_tables_and_source

TABLE_NAMES = ['ACCOUNTS', 'AD_GROUPS']
SOURCE_NAMES = ['GOOGLE_ADS', 'GOOGLE_ADS_2']


def test_get_base_tables_and_source():
    model_path = os.path.join(os.path.dirname(
        os.path.realpath(__file__)), 'test_data', 'test_sources.yml')
    table_names, source_name = get_base_tables_and_source(model_path, 0)
    assert table_names == TABLE_NAMES
    assert source_name == SOURCE_NAMES[0]
    _, source_name = get_base_tables_and_source(model_path, 1)
    assert source_name == SOURCE_NAMES[1]


def test_generate_base_model():
    pass

ls = ['a','b']
list(map(str.upper, ls))