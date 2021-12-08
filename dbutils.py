import csv
import sqlite3
from sqlite3 import Error

class DBUtil:
    
    def __init__(self):
        pass
        
    def create_connection(self):
        try:
            self.conn = sqlite3.connect(self.db)
        except Error:
            raise

    def execute_command(self, command_str):
        try:
            c = self.conn.cursor()
            c.execute(command_str)
        except Error:
            raise
            
    def get_cols_list(self):
        raise NotImplementedError
    
    def read_file_create_data_str(self):
        raise NotImplementedError
        
    def generate_create_table_command_str(self):
        return 'CREATE TABLE IF NOT EXISTS {} ({});'.format(self.table, self.schema)
    
    def generate_insert_command_str(self):
        return 'INSERT INTO {} {} VALUES {}'.format(self.table, str(tuple(self.cols)).replace("'", ''), self.data)
    
    def generate_schema_str(self, primary_key=None, dtypes_list=None):
        if not primary_key:
            primary_key = input('Enter the exact column name of the primary key:\n')
        name_and_type_list = []
        i = 0
        for col in self.cols:
            base_str = col + ' '
            name_and_type_list.append(
                base_str + (dtypes_list[i] if dtypes_list else 'text') + (' PRIMARY KEY' if col == primary_key else '')
            )
            i += 1
        return str(name_and_type_list).strip('[]').replace("'", '')
    
    def add_data(self):
        self.create_connection()
        create_table_str = self.generate_create_table_command_str()
        insert_data_str = self.generate_insert_command_str()
        try:
            self.execute_command(create_table_str)
            self.execute_command(insert_data_str)
        except Error:
            raise
        self.conn.commit()
        self.conn.close()
        return
            
class CSVUtil(DBUtil):
    
    def __init__(self, db_name, table_name, abs_file_path, header=True, primary_key=None):
        super().__init__()
        self.db = db_name + '.db'
        self.table = table_name
        self.path = abs_file_path
        self.cols = (header if not header == True else self.get_cols_list())
        self.schema = self.generate_schema_str(primary_key=primary_key)
        self.data = self.read_file_create_data_str()

    def get_cols_list(self):
        with open(self.path, newline='') as f:
            reader = csv.reader((line.replace('\0','') for line in f)) # filters out null bytes
            try:
                for row in reader:
                    return row
                    break
            except Exception:
                raise
        
    def read_file_create_data_str(self):
        with open(self.path, newline='') as f:
            reader = csv.reader((line.replace('\0','') for line in f)) # filters out null bytes
            try:
                i = 0
                base_str = str()
                for row in reader:
                    if not i:
                        pass
                    else:
                        base_str += '({}),'.format(str(row).strip('[]'))
                    i += 1
                return base_str[:-1] + ';'
            except:
                raise Exception('Error at line {}.'.format(reader.line_num))
