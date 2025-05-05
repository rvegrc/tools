
import pandas as pd
import clickhouse_connect
from typing import List, Dict, Any, Optional

class DbTools:
    def __init__(self, data_path: str, tmp_path: str, client: clickhouse_connect.get_client = None):
        self.data_path = data_path
        self.tmp_path = tmp_path
        self.client = client

    def table_field_names_types(self, df: pd.DataFrame, iana_timezone: str='Etc/GMT-3') -> Dict[str, str]:
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
                    iana_timezone: str='Etc/GMT-3', fields_comments: Dict[str,str]=None) -> Dict[str, Dict[str, str]]:
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
               
    
    def upload_to_clickhouse(self, df: pd.DataFrame, db: str, table: str, iana_timezone: str='Etc/GMT-3', fields_comments: Dict[str,str]=None) -> dict:
        """Upload data from pd.df to Clickhouse db
        return dict with fields without comments
        """
        no_comments = self.create_table_in_db(df, db, table, iana_timezone, fields_comments)
        
        self.client.insert_df(f'{db}.{table}', df)
        print(f"Data uploaded to {db}.{table}")

        return no_comments
 
    # def update_one_db_table(self, dfs_old: SparkDataFrame, dfs_new: SparkDataFrame, db_name, table_name, driver) -> None:
    #     """Update data in one clickhouse table

    #     Args:
    #         dfs_old (spark dataframe): old data from clickhouse
    #         dfs_new (spark dataframe):  new data from csv file
    #         db_name (str): db name
    #         table_name (str): table name
    #         driver (str): clickhouse driver for upload data
    #     """    
    #     if self.spark.sql(f'SELECT count(*) FROM {db_name}.{table_name}') != 0:
    #         # Check if the schemas of the two dataframes are the same
    #         dfs_new = SparkTools.check_upd_schemas(dfs_old, dfs_new)
        
    #         # Find the difference between the two dataframes
    #         dfs_subtract = dfs_new.subtract(dfs_old)

    #         # Find the intersection of IDs between the two dataframes for find updated rows
    #         id_intersect = dfs_new.select('id').intersect(dfs_old.select('id'))

    #         # Check if there are any updated rows
    #         if id_intersect.count() >= 1:
                
    #             # Truncate the tmp.id table
    #             self.spark.sql('truncate table tmp.id')
                
    #             # Insert the data from id pandas df into tmp.id
    #             id_intersect.CreateOrReplaceTempView('id_intersect')
    #             self.spark.sql("insert into table tmp.id select * from id_intersect")

    #             # Delete the rows from the table where the id is id of updated rows
    #             self.spark.sql(f'ALTER TABLE {db_name}.{table_name} DELETE WHERE id IN (SELECT id FROM tmp.id)')

    #             # upload data to clickhouse
    #             self.upload_data(dfs_subtract, db_name, table_name, driver)
    #             # there are no updated rows
    #         else:
    #             self.upload_data(dfs_new, db_name, table_name, driver)
    #     else:
    #         self.upload_data(dfs_new, db_name, table_name, driver)
