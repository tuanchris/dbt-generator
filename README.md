# dbt-generator

This package helps in generating the base models and transform them in bulk. For sources with 10+ models, this package will save you a lot of time by generating base models in bulk and transform them for common fields. Using this package is a great way to start your modeling or onboarding new sources.

## Installation

To use this package, you need dbt installed with a profile configured. You will also need to install the code-gen package from dbt Hub. Add the following to the packages.yml file in your dbt repo and run `dbt deps` to install dependencies.

```
packages:
  - package: dbt-labs/codegen
    version: 0.4.0
```

Install the package in the same environment with your dbt installation by running: 

```bash
pip install dbt-generator
```

This package should be executed inside your dbt repo.

## Generate base models

To generate base models, use the `dbt-generator generate` command. This is a wrapper around the `codegen` command that will generate the base models. This is especially useful when you have a lot of models, and you want to generate them all at once. 

```
Usage: dbt-generator generate [OPTIONS]

  Gennerate base models based on a .yml source

Options:
  -s, --source-yml PATH   Source .yml file to be used
  -o, --output-path PATH  Path to write generated models
  -m, --model STRING      Model name
  -c, --custom_prefix.    Enter a Custom String Prefix for Model Filename
  --model-prefix BOOLEAN  optional prefix of source_name + "_" to the resulting modelname.sql to avoid model name collisions across sources 
  --source-index INTEGER  Index of the source to generate base models for
  --materialized STRING    Optional parameter to set the materialization strategy (e.g. table, view, incremental')
  --help                  Show this message and exit.
```

### Example

```bash
dbt-generator generate -s ./models/source.yml -o ./models/staging/source_name/
```

This will read in the `source.yml` file and generate the base models in the `staging/source_name` folder. If you have multiple sources defined in your `yml` file, use the `--source-index` flag to specify which source you want to generate base models for.

## Transform base models using a custom YAML file

For the same source, you often have consistent naming conventions between tables. For example, the `created_at` and `modified_at` fields are often named the same for all tables. Changing all these fields to common values across different sources is a best practice. However, doing that for all the date columns in 10+ tables is a pain.

With this package, you can write a `transforms.yml` file that will be read in (the `.yml` file can be named anything). This file will contain the transforms that you want to apply to all the base models. You can just rename the fields in the base models or apply a custom SQL select to the transformed fields. 

```
Usage: dbt-generator transform [OPTIONS]

  Transform base models in a directory using a transforms.yml file

Options:
  -m, --model-path PATH       The path to models
  -t, --transforms-path PATH  Path to a .yml file containing transformations
  -o, --output-path PATH      Path to write transformed models to
  --drop-metadata BOOLEAN     (default=False) optionally drop source columns prefixed with "_" if that designates metadata columns not needed in target
  --case-sensitive BOOLEAN    (default=False) treat column names as case-sensitive - otherwise force all to lower
  --help                      Show this message and exit.
```

## Transform base models using pre-built configs 
Supported data warehouse: 
* BigQuery: bq_transform 
* Snowflake: sf_transform

```
Usage: dbt-generator bq-transform/sf-transform [OPTIONS]

  Transform base models in a directory for BigQuery source

Options:
  -m, --model-path PATH        The path to models
  -o, --output-path PATH       Path to write transformed models to
  --drop-metadata BOOLEAN      (default=False) optionally drop source columns prefixed with "_" if that designates metadata columns not needed in target
  --case-sensitive BOOLEAN     (default=False) treat column names as case-sensitive - otherwise force all to lower
  --split-columns BOOLEAN      Split column names. E.g. currencycode =>
                               currency_code
  --id-as-int BOOLEAN          Convert id to int
  --convert-timestamp BOOLEAN  Convert timestamp to datetime
  --help                       Show this message and exit.
```

### Example

```yaml
ID:
  name: ID
  sql: CAST(ID as INT64)
CREATED_TIME:
  name: CREATED_AT
UPDATED_TIME:
  name: MODIFIED_AT
DATE_START:
  name: START_AT
DATE_STOP:
  name: STOP_AT
```

This `.yml` file when applied to all models in the `staging/source_name` folder will cast all `ID` field to INT64 and rename all the date columns to a value in the `name` key. For example, `CREATED_TIME` will be renamed to `CREATED_AT` and `DATE_START` will be renamed to `START_AT`. If no `sql` is provided, the package will just rename the field. If a `sql` is provided, the package will execute the SQL and rename the field using the `name` key.

```bash
dbt-generator transform -m ./models/staging/source_name/ -t ./transforms.yml
```

This will transform all models in the `staging/source_name` folder using the `transforms.yml` file. You can also drop the metadata by setting the `drop-metadata` flag to `true` (dropping columns start with `_`). The `--case-sensitive` flag will determine if the transforms will use case-sensitive names or not.

## Limitations

Here are some of the limitations of the current release. If you want to contribute, please open an issue or a pull request.

* Transforms only works with model generated with the code-gen package. 
* You cannot transform a model that has already been transformed
*     - transformation logic assumes base model contains just a list of column names with no aliases or SQL logic added
* You cannot use wild card in fields selection for transforms (e.g. `*_id`)
* Limited test coverage
* No error handling yet
