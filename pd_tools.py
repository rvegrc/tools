import pandas as pd
import os
from IPython.display import display
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

def dict_from_dfs(path_to_data):
    """Read all csv files from the folder and create a dictionary with dataframes

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

def df_info(df):
    '''Print info and head of dataframe'''
    print(df.info())
    display('First 5 rows in df', df.head())
    print('Null values in df', df.isnull().sum())

def get_sheet_names(file_path):
    '''Function to get the names of the sheets in the excel file'''
    xl = pd.ExcelFile(file_path)
    return xl.sheet_names

def add_value_station_name(df, null_col='station_code'):
    '''Add value of station name to the previous row if station code is null'''
    for i in range(len(df)):
        if pd.isnull(df.iloc[i, 0]):
            df.iloc[i - 1, 1] += ' ' + str(df.iloc[i, 1])
    
    df = df.dropna(subset=[null_col])
    return df

def check_null_dupl(df):
    '''Check null and duplicates in df'''
    print(f'nulls = \n {df.isnull().sum()} \n')
    print(f'duplicated = {df.duplicated().sum()}')

def upper_names(file_name_geo):
    '''Upper letters in railway_name and station_name columns'''
    df = pd.read_csv(f'geo/{file_name_geo}.csv')
    df[['railway_name', 'station_name']] = df[['railway_name', 'station_name']].apply(lambda x: x.str.upper())
    return df

def drop_cols_one_uniq(df):
    '''Drop columns with only one unique value'''
    drop_cols = [col for col in df.columns if df[col].nunique() == 1]
    df.drop(columns=drop_cols, inplace=True)
    return df

def replace_date_col(dict_of_dfs, path_new_data, dcol_name):
    """replace value in "date_end" column to 2149-06-06 if it's more than 2149-06-06(max clickhouse date)
    and save the new dataframe to the same csv file

    Args:
        dict_of_dfs (dict): dict of dataframes created with "dict_from_dfs" function
        path_new_data (str): path to the folder where new dataframes will be saved
        col_name (str): name of the column with date
    """ 
    for key in dict_of_dfs.keys():
        df = dict_of_dfs[key]
        cols = df.columns
        for col in cols:
            if col == dcol_name:
                for i in range(len(df[col])):
                    # for date more then 2149-06-06 (for clickhouse). in postgresql the max date is 9999-12-31
                    try:
                        if datetime.strptime(df[col][i], '%Y-%m-%d') > datetime.strptime('2149-06-06', '%Y-%m-%d'):
                            df.loc[i, col] = pd.to_datetime('2149-06-06').date()
                            df.to_csv(f'{path_new_data}/{key}.csv', index=False)
                    except:
                        if df[col][i] > pd.to_datetime('2149-06-06').date():
                            df.loc[i, col] = pd.to_datetime('2149-06-06').date()
                            df.to_csv(f'{path_new_data}/{key}.csv', index=False)

def plot_hist_mm_lines(values: pd.Series, name: str, measure: str):
    """Plots histogram with mean and median lines.

    Args:
        values (pd.Series): values to plot
        name (str): name of the values
        measure (str): measure of the values
    """    
    plt.figure(figsize=(12, 6))
    # Sturges’ Rule to determine the optimal number of bins to use in a histogram. source https://www.statology.org/sturges-rule/
    sns.histplot(values, kde=True, bins='sturges')
    plt.axvline(values.mean(), color='r', linestyle='--', label=f'Mean value of {name}: {values.mean():.4f}{measure}')
    plt.axvline(values.median(), color='g', linestyle='--', label=f'Median value of {name}: {values.median():.4f}{measure}')
    # max value of the distribution
    plt.axvline(values.max(), color='b', linestyle='--', label=f'Max value of {name}: {values.max():.4f}{measure}')
    # min value of the distribution
    plt.axvline(values.min(), color='y', linestyle='--', label=f'Min value of {name}: {values.min():.4f}{measure}')
    plt.title(f'{name} distribution') 
    plt.ylabel(f'Frequency, sum: {values.count()}')
    plt.xlabel(f'{name}, {measure}')
    plt.legend()
    # Adjust layout to prevent clipping
    plt.tight_layout()

# rewrite for pandas
def get_feature_importances(model, train, target, tmp_path):
    '''Get feature importances from the model and save it to csv file
    and plot feature importances'''
    fi = pd.Series(model.feature_importances_)
    # Normalize feature importances
    feature_importances = [i / fi.sum() for i in fi]
    # add feature names
    features_cols = train.drop(columns=target).columns
    feature_importances = pd.DataFrame(
        list(zip(features_cols, feature_importances)),
        columns=["feature", "importance"]
    )
    # sort by importance
    feature_importances = feature_importances.sort_values(by="importance", ascending=False)
    feature_importances.to_csv(f'{tmp_path}/feature_importances_{target}.csv')

    sns.barplot(x=feature_importances['importance'], y=feature_importances['feature'])
     # Adjust layout to prevent clipping
    plt.tight_layout()