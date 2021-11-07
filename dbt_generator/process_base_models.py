import os
import yaml


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

class ProcessBaseQuery:
    def __init__(self, sql_file, transforms_file, drop_metadata=True, case_sensitive=False):
        self.sql_file = sql_file
        self.transforms_file = transforms_file
        self.drop_metadata = drop_metadata
        self.case_sensitive = case_sensitive
        self.open_query()
        self.get_columns()
        self.load_transforms()
    
    def open_query(self):
        file = open(self.sql_file).read()
        self.lines = file.split('\n')

    def get_columns(self):
        lines = self.lines
        self.start_index = [line.lower().strip() for line in lines].index('select') + 1
        self.end_index = [line.lower().strip() for line in lines].index('from source')
        columns = lines[self.start_index:self.end_index]
        cleansed_cols = list(filter(lambda x: x != '', [col.replace(',', '').strip() for col in columns]))
        self.columns = cleansed_cols
    
    def load_transforms(self):
        transforms_file = open(self.transforms_file)
        transforms = yaml.load(transforms_file, Loader=yaml.FullLoader)
        if not self.case_sensitive:
            transforms =  dict((k.lower(), v) for k, v in transforms.items())
        self.transforms = transforms
        
    def remove_metadata(self):
        self.columns = [col for col in self.columns if col[0] != '_']
    
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

    def process_sql(self):
        if self.drop_metadata:
            self.remove_metadata()
        self.process_transforms()
        columns = ['        ' + col for col in self.columns]
        columns_text = ',\n'.join(columns) + '\n'
        return columns_text

    def write_file(self, path):
        columns_text = self.process_sql()
        query_header = '\n'.join(self.lines[:self.start_index])
        query_footer = '\n'.join(self.lines[self.end_index:])
        query = query_header + '\n' + columns_text + '\n' + query_footer
        
        file = open(path, 'w')
        file.write(query)
        print(f'Processed model {path}')
