import os
from dbutils import *

db_name = 'social'
dirs = ['comment_info_json', 'comment_text', 'post_meta']
dir_paths = [os.path.abspath('./DATA/' + path) for path in dirs]
tables = ['something', 'text_table', 'something']
pkeys = ['something', 'h_id', 'something']
            # not adding method to check if data already in table, just running insert statement for every csv file
        # elif '.json' in file:
        #     json_conn = JSONUtil(*args)
def main():
    for dir_path, table, pkey in zip(dir_paths, tables, pkeys):
        for file in os.listdir(dir_path):
            file_path = os.path.join(dir_path, file)
            args = db_name, table, file_path
            kwargs = {'primary_key': pkey}
            if '.csv' in file and '.crc' not in file:
                csv_conn = CSVUtil(*args, **kwargs)
                csv_conn.add_data()

if __name__ == '__main__':
    main()