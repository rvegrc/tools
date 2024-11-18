
import pandas as pd

def sql_create_table_from_pd(df: pd.DataFrame, db: str, table: str):
    """Create sql to create table in Clickhouse from pandas dataframe

    Args:
        df (dataframe): dataframe to create table from
        db (str): name of the database
        table (str): name of the table

    Returns:
        str: sql script to create table in Clickhouse
    """    
    create_table_field = ''
    # add check for nulls for each column
    for col_name in df.columns:
        # col_name = 'l_fk_modality'
        res =  f"`{col_name}` "
        type = str(df[col_name].dtype).split("'")[0].lower()
        nullable = df[col_name].isnull().sum() > 0
        if nullable:
            res += 'Nullable('
        elif type == 'object':
            res += 'String'
            if nullable:
                res += ')'
        elif 'int' in type:
            if df[col_name].min() >= 0:
                res += 'U'
            max_value = abs(df[col_name].max())
            if max_value < 256:
                res += 'Int8'
                if nullable:
                    res += ')'
            elif max_value < 65536:
                res += 'Int16'
                if nullable:
                    res += ')'
            elif max_value < 4294967296:
                res += 'Int32'
                if nullable:
                    res += ')'
            else:
                res += 'Int64'
                if nullable:
                    res += ')'
        elif 'float' in type:
            if abs(df[col_name].max()) < 3.4 * 10**38:
                res += 'Float32'
                if nullable:
                    res += ')'
            else:
                res += 'Float64'
                if nullable:
                    res += ')'
        elif 'date' in type:
            res += 'Date'
            if nullable:
                    res += ')'
        elif 'bool' in type:
            res += 'Bool'
            if nullable:
                res += ')'

        create_table_field += res + ',\n'
    
    # get sql to create table
    return f'''
    drop table if exists {db}.{table};
    
    CREATE TABLE {db}.{table} (
        {create_table_field}
    ) 
    ENGINE = MergeTree()
    ORDER BY tuple();
    '''