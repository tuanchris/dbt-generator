import os
import click
from .generate_base_models import *
from .process_base_models import *

@click.group(help='Generate and process base dbt models')
def dbt_generator():
    pass

@dbt_generator.command(help='Gennerate base models based on a .yml source')
@click.option('-s', '--source-yml', type=click.Path(), help='Source .yml file to be used')
@click.option('-o', '--output-path', type=click.Path(), help='Path to write generated models')
@click.option('--source-index', type=int, default=0, help='Index of the source to generate base models for')
def generate(source_yml, output_path, source_index):
    tables, source_name = get_base_tables_and_source(source_yml, source_index)
    for table in tables:
        file_name = table + '.sql'
        query = generate_base_model(table, source_name)
        file = open(os.path.join(output_path, file_name), 'w')
        file.write(query)

@dbt_generator.command(help='Process base models in a directory')
@click.option('-m', '--model-path', type=click.Path(), help='The path to models')
@click.option('-t', '--transforms-path', type=click.Path(), help='Path to a .yml file containing transformations')
@click.option('--drop-metadata', type=bool, help='The drop metadata flag', default=True)
@click.option('--case-sensitive', type=bool, help='The case sensitive flag', default=False)
def process(model_path, transforms_path, drop_metadata, case_sensitive):
    sql_files = get_sql_files(model_path)
    for sql_file in sql_files:
        pbq = ProcessBaseQuery(os.path.join(model_path, sql_file), transforms_path, drop_metadata, case_sensitive)
        pbq.write_file(os.path.join(model_path, sql_file))

if __name__ == '__main__':
    dbt_generator()