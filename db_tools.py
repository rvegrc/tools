
import pandas as pd
import clickhouse_connect
from typing import List, Dict, Any, Optional
from .pd_tools import df_diff
import os
import datetime

class DbTools:
    def __init__(self, data_path: str, tmp_path: str, client: clickhouse_connect.get_client = None):
        self.data_path = data_path
        self.tmp_path = tmp_path
        self.client = client

    def table_field_names_types(self, df: pd.DataFrame, iana_timezone: str='UTC') -> Dict[str, str]:
        """Get fields and their types 
        from pd.DataFrame for db table creation
        """

        field_types = {}
        
        for col_name in df.columns:
            col_type = str(df[col_name].dtype).lower()
            nullable = df[col_name].isnull().any()
            

            if 'object' in col_type:
                field_types[col_name] = 'String'
            elif 'int' in col_type:
                min_value = df[col_name].min()
                max_value = df[col_name].max()

                if min_value >= 0:
                    if max_value < 128:
                        field_types[col_name] = 'UInt8'
                    elif max_value < 32767:
                        field_types[col_name] = 'UInt16'
                    elif max_value < 2147483647:
                        field_types[col_name] = 'UInt32'
                    else:
                        field_types[col_name] = 'Int64'
                else:
                    if min_value >= -128 and max_value < 127:
                        field_types[col_name] = 'Int8'
                    elif min_value >= -32768 and max_value < 32767:
                        field_types[col_name] = 'Int16'
                    elif min_value >= -2147483648 and max_value < 2147483647:
                        field_types[col_name] = 'Int32'
                    else:
                        field_types[col_name] = 'Int64'

            elif 'float' in col_type:
                max_value = abs(df[col_name].max())
                field_types[col_name] = 'Float32' if max_value < 3.4 * 10**38 else 'Float64'
    
            elif 'datetime' in col_type:
                field_types[col_name] = f"DateTime64(6, '{iana_timezone}')"
            elif 'bool' in col_type:
                field_types[col_name] = 'Bool'
            else:
                field_types[col_name] = 'String'  # Default fallback type
                  
            if nullable:
                field_types[col_name] = f"Nullable({field_types[col_name]})"
                

        return field_types

    def create_table_in_db(self, df: pd.DataFrame, db: str, table: str, 
                    iana_timezone: str='UTC', fields_comments: Dict[str,str]=None) -> Dict[str, Dict[str, str]]:
        '''Create table in ClickHouse db from pd.DataFrame and return tables with fields without comments
        fields_comments: dict: dictionary with comments for fields in table
        '''
        # check if table exists
        if self.client.command(f'exists {db}.{table}') == 1:
            print(f"Table {db}.{table} already exists")
            
            return {'no filled fields: Table already exists'}
        
        else:
                       
            # no_comments = {f'{table}': {}}

            # if fields_comments is not None:        
            #     # use field name for key in fields_comments dict
            #         for field_name in field_names_types.keys():
            #             try:
            #                 # for fields with comments
            #                 field_names_types[field_name] = field_names_types[field_name] + f" COMMENT '{fields_comments[field_name]}'"
            #             except:
            #                 # for fields without comments
            #                 no_comments[f'{table}'][f'{field_name}'] = 'No comments'
                            

            # else:
            #     no_comments[f'{table}']['all_fields'] = 'No comments'

            field_names_types = [f'{field_name} {field_type}\n' for field_name, field_type in self.table_field_names_types(df, iana_timezone).items()]

            # print(field_names_types)

            create_table_script = f"""
                create table if not exists {db}.{table} 
                (
                    {','.join(field_names_types)}
                )
                engine = MergeTree()
                order by tuple()
            """
            # print(create_table_script)
            try:
                self.client.command(create_table_script)
                print(f"Table {db}.{table} created")
            except Exception as e:
                    print(f"Error while creating table {db}.{table}\n{e}")
               
    def upload_to_clickhouse(self, df: pd.DataFrame, db: str, table: str, iana_timezone: str='UTC', fields_comments: Dict[str,str]=None) -> dict:
        '''Create table and upload data from df to Clickhouse db
        return dict with fields without comments
        '''
        
        # check if table does not exist or table is empty in db, for correct using of function
        if self.client.command(f'exists {db}.{table}') == 0 or self.client.query_df(f'select * from {db}.{table}').shape[0] == 0:
                print(f"Table {db}.{table} does not exist")
                # create table in db
                no_comments = self.create_table_in_db(df, db, table, iana_timezone, fields_comments)
                # upload data to db
                self.client.insert_df(f'{db}.{table}', df)
                print(f"Data uploaded to created {db}.{table}")
        else:
                print(f"Table {db}.{table} already exists, use 'uu_to_clickhouse' function")
                no_comments = {}

        return no_comments
 
    # def uu_to_clickhouse(self, df_new: pd.DataFrame, db: str, table: str, tmp_db: str='tmp', iana_timezone: str='UTC', fields_comments: Dict[str,str]=None):
    #     '''Upload new and update old data from df_new to Clickhouse db'''
        
    #     print(f'\nChecking table {db}.{table}...')

    #     # check if table exists in db
    #     if self.client.command(f'exists {db}.{table}') == 0:
    #         # create table in db and upload data
    #         self.upload_to_clickhouse(df_new, db, table, iana_timezone, fields_comments)
    #         print(f"Table {db}.{table} created and data uploaded")

    #     else:
    #         df_old = self.client.query_df(f'select * from {db}.{table}')

    #         new_data, updated_data = df_diff(df_new, df_old, 'id')
            
    #         if len(updated_data) > 0:
    #             # drop rows in db with id from updated_data dataframe
    #             ids_to_drop = pd.DataFrame(updated_data['id'])
                
    #             # drop old table with ids to drop
    #             self.client.command(f'drop table if exists {tmp_db}.ids_to_drop')
            
    #             # Create ids table and upload 'ids to drop' to db
    #             self.upload_to_clickhouse(ids_to_drop, tmp_db, 'ids_to_drop', iana_timezone, fields_comments)

    #             # Delete rows with ids from updated rows via join
    #             self.client.command(f'''
    #                 delete from {db}.{table}
    #                 where id in (select id from {tmp_db}.ids_to_drop)
    #                 ''')
    #             # upload updated data to db
    #             self.client.insert_df(f'{db}.{table}', updated_data)
    #             print(f'Updated data uploaded to {db}.{table}, count of updated rows:{updated_data.shape[0]}')
            
    #         else:
    #             print(f"No updated data uploaded to {db}.{table}")

            
    #         if len(new_data) > 0:
    #             # upload new data to db
    #             self.client.insert_df(f'{db}.{table}', new_data)
    #             print(f'New data uploaded to {db}.{table}, count of new rows {new_data.shape[0]}')
    #         else:
    #             print(f"No new data uploaded to {db}.{table}")


    def uu_to_clickhouse(
        self,
        df_new: pd.DataFrame,
        key_columns: List[str],
        db: str,
        table: str,
        tmp_db: str = "tmp",
        iana_timezone: str = "UTC",
        fields_comments: Dict[str, str] = None,
    ):
        """
        Upload new and update old data from df_new to Clickhouse db.
        Uses key_columns for identifying same rows.
        """

        print(f"\nChecking table {db}.{table}...")

        # check if table exists or is empty
        if self.client.command(f"exists {db}.{table}") == 0 or self.client.query_df(f"select * from {db}.{table}").shape[0] == 0:
            # create table and upload all data
            self.upload_to_clickhouse(df_new, db, table, iana_timezone, fields_comments)
            print(f"Table {db}.{table} created and data uploaded")
            return

        # fetch old data
        df_old = self.client.query_df(f"select * from {db}.{table}")

        # ignore autogenerated columns
        ignore_cols = {"id", "date"}
        common_cols = [c for c in df_old.columns if c in df_new.columns and c not in ignore_cols]

        df_new = df_new[common_cols]
        df_old = df_old[common_cols]

        # compare using df_diff   
        new_data, updated_data = df_diff(df_new, df_old, key_columns)

        # process updates
        if len(updated_data, key_columns) > 0:
            print(f"Found {len(updated_data)} updated rows")

            # delete existing rows in ClickHouse matching updated keys
            keys_to_delete = updated_data[key_columns]
            self.client.command(f"drop table if exists {tmp_db}.keys_to_delete")
            self.upload_to_clickhouse(keys_to_delete, tmp_db, "keys_to_delete", iana_timezone, fields_comments)

            self.client.command(f"""
                ALTER TABLE {db}.{table}
                DELETE WHERE ({key_columns}) IN 
                    (SELECT {key_columns} FROM {tmp_db}.keys_to_delete)
            """)

            # insert updated rows
            self.client.insert_df(f"{db}.{table}", updated_data)
            print(f"Updated rows uploaded to {db}.{table}")

        else:
            print("No updated rows found")

        # process inserts
        if len(new_data) > 0:
            self.client.insert_df(f"{db}.{table}", new_data)
            print(f"Inserted {len(new_data)} new rows into {db}.{table}")
        else:
            print("No new rows found")



    
    def save_db_tables_to_parquet(self, db: str, parent_dir: str):
        """Save all tables from ClickHouse db to parquet files in parent_dir/bu/db_data/date_save"""
        # create date for saving data
        date_save = datetime.datetime.now().strftime('%Y-%m-%d')
    
        for table in self.client.query_df(f'show tables from {db}')['name']:
            # make directory for saving data if not exists
            os.makedirs(f'{parent_dir}/bu/db_data/{date_save}', exist_ok=True)
            self.client.query_df(f'select * from {db}.{table}').to_parquet(f'{parent_dir}/bu/db_data/{date_save}/{db}__{table}.parquet', index=False)
            print(f'Table {db}__{table} saved')