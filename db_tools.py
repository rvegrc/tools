
import pandas as pd
import clickhouse_connect

class DbTools:
    def __init__(self, data_path: str, tmp_path: str, client: clickhouse_connect.get_client = None):
        self.data_path = data_path
        self.tmp_path = tmp_path
        self.client = client


    def sql_create_table(df: pd.DataFrame, db: str, table: str) -> str:
        """Create sql script for create table in Clickhouse db from pandas dataframe
        """    
        # create_table_field = ''
        # # add check for nulls for each column
        # for col_name in df.columns:
        #     # col_name = 'l_fk_modality'
        #     res =  f"`{col_name}` "
        #     type = str(df[col_name].dtype).split("'")[0].lower()
        #     nullable = df[col_name].isnull().sum() > 0
        #     if nullable:
        #         res += 'Nullable('
        #     elif type == 'object':
        #         res += 'String'
        #         if nullable:
        #             res += ')'
        #     elif 'int' in type:
        #         if df[col_name].min() >= 0:
        #             res += 'U'
        #         max_value = abs(df[col_name].max())
        #         if max_value < 256:
        #             res += 'Int8'
        #             if nullable:
        #                 res += ')'
        #         elif max_value < 65536:
        #             res += 'Int16'
        #             if nullable:
        #                 res += ')'
        #         elif max_value < 4294967296:
        #             res += 'Int32'
        #             if nullable:
        #                 res += ')'
        #         else:
        #             res += 'Int64'
        #             if nullable:
        #                 res += ')'
        #     elif 'float' in type:
        #         if abs(df[col_name].max()) < 3.4 * 10**38:
        #             res += 'Float32'
        #             if nullable:
        #                 res += ')'
        #         else:
        #             res += 'Float64'
        #             if nullable:
        #                 res += ')'
        #     elif 'date' in type:
        #         res += 'Date'
        #         if nullable:
        #                 res += ')'
        #     elif 'bool' in type:
        #         res += 'Bool'
        #         if nullable:
        #             res += ')'

        #     create_table_field += res + ',\n'
        
        # # get sql script for create table
        # return f'''
        # drop table if exists {db}.{table};
        
        # CREATE TABLE {db}.{table} (
        #     {create_table_field}
        # ) 
        # ENGINE = MergeTree()
        # ORDER BY tuple();
        # '''

    def sql_create_table(self, df: pd.DataFrame, db: str, table: str) -> None:
            """Create table in ClickHouse db from pd.DataFrame
            db: str: database name
            table: str: table name
           """
            
            create_table_fields = []
            
            for col_name in df.columns:
                res = f"`{col_name}` "
                col_type = str(df[col_name].dtype).lower()
                nullable = df[col_name].isnull().any()
                
                if 'object' in col_type:
                    res += 'String'
                elif 'int' in col_type:
                    min_value = df[col_name].min()
                    max_value = df[col_name].max()

                    if min_value >= 0:
                        res += 'U'
                    
                    if max_value < 256:
                        res += 'Int8'
                    elif max_value < 65536:
                        res += 'Int16'
                    elif max_value < 4294967296:
                        res += 'Int32'
                    else:
                        res += 'Int64'
                elif 'float' in col_type:
                    max_value = abs(df[col_name].max())
                    res += 'Float32' if max_value < 3.4 * 10**38 else 'Float64'
                elif 'date' in col_type:
                    res += 'Date'
                elif 'bool' in col_type:
                    res += 'Bool'
                else:
                    res += 'String'  # Default fallback type
                
                # split col name and col type
                if nullable:
                    # split col name and col type from res
                    res = res.split(' ')[0] + f" Nullable({res.split(' ')[1]})"

                create_table_fields.append(res)

            create_table_sql = f"""
                create table if not exists {db}.{table} (
                    {', '.join(create_table_fields)}
                ) 
                engine = MergeTree()
                order by tuple();
            """

            return create_table_sql
    
    def upload_to_clickhouse(self, df: pd.DataFrame, db: str, table: str) -> None:
        """Upload data from pd.df to Clickhouse db
        """
        if self.client:
            self.client.insert_df(f'{db}.{table}', df)
        else:
            print('Client is not defined')

