import findspark
findspark.init()
import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import clickhouse_connect



from pyspark.sql import DataFrame as SparkDataFrame

from pyspark.sql import functions as F

def check_nn_spark(dfs):
    '''Function to check null and nan values in each column spark df'''
    
    dfs.select([F.count(F.when(F.isnull(c) | F.isnan(c), c)).alias(c) for c in dfs.columns]).show()

# write def for update data from xls to db

def dict_from_spark_dfs(path_to_data):
    """Read all csv files from the folder and create a dictionary with spark dataframes

    Args:
        path_to_data (str): path to the folder with csv files

    Returns:
        dict: dictionary with dataframes
    """        
    files_list = os.listdir(path_to_data)
    file_names_list = [x.replace('.csv', '') for x in files_list if x.endswith('.csv')]
    # file_name = list(map(lambda x: x.replace('.csv', ''), file_names_list)) # alternative way to create file_names_list
    dict_dfs = {}
    for name, csv_file in zip(file_names_list, files_list):
        dict_dfs[name] = pd.read_csv(path_to_data + '/' + csv_file)
    return dict_dfs

    
    
def check_upd_schemas(dfs_old: SparkDataFrame, dfs_new: SparkDataFrame) -> SparkDataFrame:
    """Check if the schemas of the two dataframes are the same
        and if not, update the schema of the new dataframe to match the old one

    Args:
        dfs_old: df from the database
        dfs_new: df from the csv file
    """    
    dict_old = {}
    [dict_old.update({col: dtype}) for col, dtype in dfs_old.dtypes]

    dict_new = {}
    [dict_new.update({col: dtype}) for col, dtype in dfs_new.dtypes]

    for key in dict_old.keys():
        if dict_old[key] != dict_new[key]:
            dfs_new = dfs_new.withColumn(key, F.col(key).cast(dict_old[key]))
    return dfs_new

def get_f_imp_spark(model, features_cols:list, target:str, tmp_path:str):
    '''Get feature importances from the model and save it to the csv file and plot to the png file'''
    # Get feature importance
    fi = pd.Series(model.stages[-1].getFeatureImportances())
    
    # Normalize feature importances
    feature_importances = [i / fi.sum() for i in fi]
    
    # add feature names
    feature_importances = pd.DataFrame(list(zip(features_cols, feature_importances)), columns=["feature", "importance"])
    
    # sort by importance
    feature_importances = feature_importances.sort_values(by="importance", ascending=False)
    feature_importances.to_csv(f'{tmp_path}/feature_importances_{target}.csv')

    sns.barplot(x=feature_importances['importance'], y=feature_importances['feature'])
    plt.title('Feature importances')
    plt.tight_layout()
    plt.savefig(f'{tmp_path}/feature_importances_{target}.png')
    plt.clf()


def upload_data(dfs: SparkDataFrame, db_name: str, table_name: str, driver: str) -> None:
    """Upload data in clickhouse table 

    Args:
        dfs (spark dataframe): dataframe with new data
        db_name (str): name of db
        table_name (str): name of table
        driver (str): clickhouse driver for upload data
    """
     # Check if the schemas from database and uploading dataframes are the sam
    dfs_db = spark.sql(f'select * from {db_name}.{table_name}')
   

    dfs = check_upd_schemas(dfs_db, dfs)
   
    if driver == 'jdbc':       
        (
            dfs.write.format("jdbc")
            .option("url", f"jdbc:clickhouse://{CH_IP}:9000")
            # .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
            .option("driver", "com.github.housepower.jdbc.ClickHouseDriver")
            .option("dbtable", f"{db_name}.{table_name}") # table name
            .option("user", CH_USER)
            .option("password", CH_PASS)
            .option("isolationLevel", "NONE")
            .mode("append")
            .save()
        )
    # client clickhouse connect
    else:
        client = clickhouse_connect.get_client(host=CH_IP, port=8123, username=CH_USER, password=CH_PASS)
        # convert datetime column
        client.insert_df(f'{db_name}.{table_name}', dfs.toPandas())


def update_one_db_table(dfs_old, dfs_new, db_name, table_name, driver):
    """Update data in one clickhouse table

    Args:
        dfs_old (spark dataframe): old data from clickhouse
        dfs_new (spark dataframe):  new data from csv file
        db_name (str): db name
        table_name (str): table name
        driver (str): clickhouse driver for upload data
    """    
    if client.command(f'SELECT count(*) FROM {db_name}.{table_name}') != 0:
        # Check if the schemas of the two dataframes are the same
        dfs_new = spark_tools.check_upd_schemas(dfs_old, dfs_new)
    
        # Find the difference between the two dataframes
        dfs_subtract = dfs_new.subtract(dfs_old)

        # Find the intersection of IDs between the two dataframes for find updated rows
        id_intersect = dfs_new.select('id').intersect(dfs_old.select('id'))

        # Check if there are any updated rows
        if id_intersect.count() >= 1:
            
            # Truncate the tmp.id table
            client.command('truncate table tmp.id')
            
            # Insert the data from id pandas df into tmp.id
            client.insert("tmp.id", id_intersect.toPandas())

            # Delete the rows from the table where the id is id of updated rows
            client.command(f'ALTER TABLE {db_name}.{table_name} DELETE WHERE id IN (SELECT id FROM tmp.id)')

            # upload data to clickhouse
            upload_data(dfs_subtract, db_name, table_name, driver)
            # there are no updated rows
        else:
            upload_data(dfs_new, db_name, table_name, driver)
    else:
        upload_data(dfs_new, db_name, table_name, driver)

def update_db_tables(db_name: str, path_data_from: str, driver: str) -> None:
    """Update tables in db from csv files in folder,
        were not uploaded table_names saving in list 'tables_w_err'

    Args:
        db_name: name of db
        path_data_from: path to folder with csv files with data
        driver: clickhouse driver for upload data
    """    
    files = os.listdir(path_data_from)
    # list of tables with errors when upload
    tables_w_err = []
    for filename in files:
        table_name = filename.split('.')[0]
        dfs_old = spark.sql(f'select * from {db_name}.{table_name}')
        # quote='"', escape='"', multiLine=True for correct read json column
        dfs_new = spark.read.csv(f'{path_data_from}/{filename}', header=True, inferSchema=True, quote='"', escape='"', multiLine=True)
        try:
            update_one_db_table(dfs_old, dfs_new, db_name, table_name, driver)
        except:
            tables_w_err.append(table_name)
    print(f"Tables with errors: {tables_w_err}")