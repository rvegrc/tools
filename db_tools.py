
import pandas as pd
import clickhouse_connect
from typing import List, Dict, Any, Optional

class DbTools:
    def __init__(self, data_path: str, tmp_path: str, client: clickhouse_connect.get_client = None):
        self.data_path = data_path
        self.tmp_path = tmp_path
        self.client = client

    def table_field_names_types(self, df: pd.DataFrame, iana_timezone: str=None) -> Dict[str, str]:
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
            elif 'date' in col_type:
                field_types[col_name] = 'Date'
            elif 'time' in col_type:
                field_types[col_name] = f"DateTime64(6, '{iana_timezone}')"
            elif 'bool' in col_type:
                field_types[col_name] = 'Bool'
            else:
                field_types[col_name] = 'String'  # Default fallback type
                  
            if nullable:
                field_types[col_name] = f"Nullable({field_types[col_name]})"
                

        return field_types

    def create_table_in_db(self, df: pd.DataFrame, db: str, table: str, 
                    iana_timezone: str=None, fields_comments: Dict[str,str]=None) -> Dict[str, Dict[str, str]]:
        """Create table in ClickHouse db from pd.DataFrame and return tables with fields without comments
        db: str: database name
        table: str: table name
        fields_comments: dict: dictionary with comments for fields in table
        """
        field_names_types = self.table_field_names_types(df, iana_timezone)

        no_comments = {f'{table}': {}}

        if fields_comments is not None:        
            # use field name for key in fields_comments dict
                for field_name in field_names_types.keys():
                    try:
                        # for fields with comments
                        field_names_types[field_name] = field_names_types[field_name] + f" COMMENT '{fields_comments[field_name]}'"
                    except:
                        # for fields without comments
                        no_comments[f'{table}'][f'{field_name}'] = 'No comments'
                        

        else:
            no_comments[f'{table}']['all_fields'] = 'No comments'

        field_names_types = [f'{field_name} {field_type}\n' for field_name, field_type in field_names_types.items()]


        create_table_script = f"""
            create table if not exists {db}.{table} 
            (
                {','.join(field_names_types)}
            )
            engine = MergeTree()
            order by tuple();
        """
        # print(create_table_script)
        try:
            #check if table is exists
            if self.client.command(f'exists {db}.{table}') == 0:            
                self.client.command(create_table_script)
                print(f"Table {db}.{table} created")
            else:
                print(f'Table {db}.{table} is exists')
        except Exception as e:
                print(f"Error while creating table {db}.{table}\n{e}")
        
        return no_comments
               
    
    def upload_to_clickhouse(self, df: pd.DataFrame, db: str, table: str) -> None:
        """Upload data from pd.df to Clickhouse db
        """
        try:
            if self.client:
                self.client.insert_df(f'{db}.{table}', df)
            else:
                print('Client is not defined')
        except Exception as e:
                print(f"Error while upload data to {db}.{table}\n{e}")
        
        
        if self.client:
            self.client.insert_df(f'{db}.{table}', df)
        else:
            print('Client is not defined')

