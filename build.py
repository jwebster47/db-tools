import os
from dbutils import *
        
def main(db_name, data_dir, table_dirs, tables, pkeys):
    table_dir_paths = [os.path.abspath('./' + data_dir +'/' + path)
                      for path in table_dirs]
    for dir_path, table, pkey in zip(table_dir_paths, tables, pkeys):
        for file in os.listdir(dir_path):
            file_path = os.path.join(dir_path, file)
            args = db_name, table, file_path
            kwargs = {'primary_key': pkey}
            if '.csv' in file:
                csv_conn = CSVUtil(*args, **kwargs)
                csv_conn.add_data()

if __name__ == '__main__':
    db_name = input('Please type the current or desired sqlite database name (exclude extension).\n')
    data_dir = input('Please type the name of the data directory in the current path containing the separate table directories.\n')
    table_dirs = input('Please type the names of the separate table directories containing the data files to upload separated by a space.\n').split(' ')
    tables = input('Please type the current or desired table names separated by a space (one for each table directory).\n').split(' ')
    pkeys = input('Please type the current or desired primary keys for each table separated by a space.\n').split(' ')
    main(db_name, data_dir, table_dirs, tables, pkeys)

# add functionality for specifying data types
# add functionality for excluding certain files
# add functionality for other file extensions e.g. json, parquet