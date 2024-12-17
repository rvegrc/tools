import findspark
findspark.init()
import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


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