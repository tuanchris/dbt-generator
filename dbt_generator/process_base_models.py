import os
import re
import yaml
import wordninja
from abc import ABC, abstractmethod


def get_sql_files(path):
    """
    Get all the sql files in the path
    :param path:
    :return:
    """
    files = []
    for file in os.listdir(path):
        if file.endswith(".sql"):
            files.append(file)
    return files


class ProcessBaseQuery(ABC):
    SQL_COLUMN_REGEX = "^[a-zA-Z_][a-zA-Z0-9_]*$"

    def __init__(self, sql_file, drop_metadata=False, case_sensitive=False):
        self.sql_file = sql_file
        self.drop_metadata = drop_metadata
        self.case_sensitive = case_sensitive
        self.open_query()
        self.get_columns()

    def open_query(self):
        '''
        Open the sql file and store the lines in a list
        '''
        file = open(self.sql_file).read()
        self.lines = file.split('\n')
        return file.split('\n')

    def get_columns(self):
        '''
        Get the columns from the sql file
        '''
        lines = self.lines
        # Get lines between select and from source statements
        try:
            self.start_index = [line.lower().strip()
                                for line in lines].index('select') + 1
            self.end_index = [line.lower().strip()
                              for line in lines].index('from source')
        except ValueError:
            raise Exception(
                f'{self.sql_file} does not contain a "select" and/or "from source" statement')
        columns = lines[self.start_index:self.end_index]

        # Strip columns and remove commas
        cleansed_cols = list(
            filter(lambda x: x != '', [col.replace(',', '').strip() for col in columns]))
        # Match only valid column names
        cleansed_cols = [col for col in cleansed_cols if re.match(
            self.SQL_COLUMN_REGEX, col)]
        self.columns = cleansed_cols
        # Remove metadata columns
        if self.drop_metadata:
            self.remove_metadata()
        # Raise exception if no columns found
        if cleansed_cols == []:
            raise Exception(f'{self.sql_file} does not contain any columns')

    def remove_metadata(self):
        self.columns = [col for col in self.columns if col[0] != '_']

    @abstractmethod
    def process_transforms():
        pass

    def process_sql(self):
        columns = ['        ' + col for col in self.columns]
        columns_text = ',\n'.join(columns) + '\n'
        return columns_text

    def process_base_models(self, path):
        self.process_transforms()
        columns_text = self.process_sql()
        query_header = '\n'.join(self.lines[:self.start_index])
        query_footer = '\n'.join(self.lines[self.end_index:])
        query = query_header + '\n' + columns_text + '\n' + query_footer

        file = open(path, 'w')
        file.write(query)
        print(f'Processed model {path}')


class ProcessBaseModelsWithTransforms(ProcessBaseQuery):
    def __init__(self, transforms_file, sql_file, drop_metadata=False, case_sensitive=False):
        super().__init__(sql_file, drop_metadata, case_sensitive)
        self.transforms_file = transforms_file
        self.load_transforms()

    def load_transforms(self):
        transforms_file = open(self.transforms_file)
        transforms = yaml.load(transforms_file, Loader=yaml.FullLoader)
        if not self.case_sensitive:
            transforms = dict((k.lower(), v) for k, v in transforms.items())
        self.transforms = transforms

    def process_transform(self, base_column, transform):
        transformed_name = transform['name']
        if 'sql' in transform.keys():
            sql = transform['sql']
        else:
            sql = base_column
        return f'{sql} as {transformed_name}'

    def process_transforms(self):
        for column in self.transforms:
            if column in self.columns:
                index = self.columns.index(column)
                transform = self.transforms[column]
                self.columns[index] = self.process_transform(column, transform)


class ProcessBaseModels(ProcessBaseQuery):
    def __init__(
        self,
        sql_file,
        drop_metadata=False,
        case_sensitive=False,
        split_columns=False,
        id_as_int=False,
        convert_timestamp=False,
    ):
        super().__init__(sql_file, drop_metadata, case_sensitive)
        self.split_columns = split_columns
        self.id_as_int = id_as_int
        self.convert_timestamp = convert_timestamp

    def split_column_name(self, column_name):
        column_name = wordninja.split(column_name)
        column_name = '_'.join(column_name)
        return column_name

    def process_transforms(self):
        processed_columns = []
        for column in self.columns:
            column_alias = column
            if not self.case_sensitive:
                column = column.lower()
            if self.split_columns and column[0] != '_':
                column_alias = self.split_column_name(column)
            if self.id_as_int and '_id' in column_alias:
                column = self.integer_convert.format(column)
            if self.convert_timestamp and (
                '_timestamp' in column_alias or
                '_date' in column_alias or
                '_at' in column_alias
            ):
                column = self.timestamp_convert.format(column)

            processed_columns.append(f'{column} as {column_alias}')
        self.columns = processed_columns


class ProcessBaseModelsBQ(ProcessBaseModels):
    timestamp_convert = 'timestamp({})'
    integer_convert = 'cast({} as int64)'


class ProcessBaseModelsSF(ProcessBaseModels):
    timestamp_convert = '{}::timestamp'
    integer_convert = '{}::integer'
