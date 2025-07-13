import pandas as pd
import os
from IPython.display import display
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

import secrets
import string
import json



def dict_dfs(path: str) -> dict:
    """Read all csv files from the folder and create a dictionary with dataframes
    """        
    files_list = os.listdir(path)
    csv_file_names = [x.replace('.csv', '') for x in files_list if x.endswith('.csv')]
    parq_file_names = [x.replace('.parquet', '') for x in files_list if x.endswith('.parquet')]
    # file_name = list(map(lambda x: x.replace('.csv', ''), file_names_list)) # alternative way to create file_names_list
    dict_dfs = {}
    for name, file in zip(csv_file_names, files_list):
        dict_dfs[name] = pd.read_csv(path + '/' + file)
    for name, file in zip(parq_file_names, files_list):
        dict_dfs[name] = pd.read_parquet(path + '/' + file)
    return dict_dfs

def not_same_types(df: pd.DataFrame, col_name: str) -> pd.Series | None:
    """
    If there are multiple dtypes in col, return a col_name with counts of each dtype.
    Null values are ignored in the check.
    If all dtypes are the same, return None and print col_name and type.    
    """
    non_null_col = df[col_name][df[col_name].notnull()]
    col_types = non_null_col.apply(type).value_counts()
    if len(col_types) > 1:
        return col_types
    else:
        print(f'Column {col_name} has only one type: {col_types.index[0]}')
        return None  


def check_text_properties(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    '''
    Classifies values in col_name as digit, alpha, special symbols, empty or etc.
    "etc" means values that are not digit, alpha, or special-symbols-only.
    '''
    series = df[col_name].astype(str).str.strip()

    is_digit = series.apply(lambda x: x.isdigit())
    is_alpha = series.apply(lambda x: x.isalpha())
    is_empty = series.apply(lambda x: x == '')
    has_special_symbols = series.apply(lambda x: bool(re.search(r'[^a-zA-Z0-9 ]', x)))

    # etc = not any of the above
    etc = ~(is_digit | is_alpha | is_empty | has_special_symbols)

    results_df = pd.DataFrame({
        'is_digit': is_digit,
        'is_alpha': is_alpha,
        'is_empty': is_empty,
        'has_special_symbols': has_special_symbols,
        'etc': etc
    })

    # Count how many values fall into each category
    summary = results_df.sum().reset_index()
    summary.columns = ['text_property', 'count']

    return summary



def df_info(df: pd.DataFrame) -> None:
    '''Print info,head and cols with zeroes, nulls and duplicates in df'''
    print(df.info())
    display(f'First 5 rows in df', df.head())
    zeroes = {}
    minus_ones = {}
    nulls = {}
    nans = {}
    nones = {}
    string_placeholders = {
        'NA': {},
        'null': {},
        'N/A': {}
    }
    duplicates_full = {}
    duplicates_by_cols = {}
    not_same_types_cols = {}
    

    for col in df.columns:
        null_count = df[col].isnull().sum()
        nan_count = df[col].isna().sum()
        none_count = df[col].apply(lambda x: x is None).sum()
        
        if null_count > 0:
            nulls[col] = null_count
        if nan_count > 0:
            nans[col] = nan_count
        if none_count > 0:
            nones[col] = none_count
        if (df[col] == -1).sum() > 0:
            minus_ones[col] = (df[col] == -1).sum()
        if (df[col] == 0).sum() > 0 and df[col].dtype not in ['object', 'datetime64[ns]', 'bool']:
            zeroes[col] = (df[col] == 0).sum()

        # Check for string placeholders
        if df[col].dtype == 'object':
            for placeholder in string_placeholders:
                count = (df[col].astype(str).str.strip().str.lower() == placeholder.lower()).sum()
                if count > 0:
                    string_placeholders[placeholder][col] = count
        
        # Count duplicates by columns except for nulls
        duplicates_by_cols[col] = df[col][df[col].notnull()].duplicated().sum()

        # Check for different types in the column
        not_same_types_col = not_same_types(df, col)
        if not_same_types_col is not None:
            not_same_types_cols[col] = not_same_types_col.to_dict()
        

    # Count full duplicates
    duplicates_full = df.duplicated().sum()

    # Convert to DataFrames
    zeroes = pd.DataFrame(zeroes, index=['zeroes'])
    minus_ones = pd.DataFrame(minus_ones, index=['minus_ones'])
    nulls = pd.DataFrame(nulls, index=['nulls'])
    nans = pd.DataFrame(nans, index=['nans'])
    nones = pd.DataFrame(nones, index=['nones'])
    na_df = pd.DataFrame(string_placeholders['NA'], index=['NA placeholder'])
    null_str_df = pd.DataFrame(string_placeholders['null'], index=['null placeholder'])
    na_slash_df = pd.DataFrame(string_placeholders['N/A'], index=['N/A placeholder'])
    duplicates_full = pd.DataFrame([duplicates_full], index=['duplicates_full'], columns=['duplicates_full'])
    duplicates_by_cols = pd.DataFrame(duplicates_by_cols, index=['duplicates_by_cols'])
    not_same_types_cols = pd.DataFrame.from_dict(not_same_types_cols, orient='index')

    # Display all
    display(duplicates_full, duplicates_by_cols, zeroes, minus_ones, nulls, nans, nones, na_df, null_str_df, na_slash_df, not_same_types_cols)

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

def replace_date_col(dict_of_dfs, dcol_name):
    """replace value in "date_finish" column to 2149-06-06 if it's more than 2149-06-06(max clickhouse date)
    and save the new dataframe to the same csv file

    Args:
        dict_of_dfs (dict): dict of pd.dataframes
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
                            # df.to_csv(f'{path_new_data}/{key}.csv', index=False)
                    except:
                        if df[col][i] > pd.to_datetime('2149-06-06').date():
                            df.loc[i, col] = pd.to_datetime('2149-06-06').date()
                            # df.to_csv(f'{path_new_data}/{key}.csv', index=False)

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
    plt.axvline(values.mean(), color='r', linestyle='--', label=f'Mean value of {name}: {values.mean():.4f} um={measure}')
    plt.axvline(values.median(), color='g', linestyle='--', label=f'Median value of {name}: {values.median():.4f} um={measure}')
    # max value of the distribution
    plt.axvline(values.max(), color='b', linestyle='--', label=f'Max value of {name}: {values.max():.4f} um={measure}')
    # min value of the distribution
    plt.axvline(values.min(), color='y', linestyle='--', label=f'Min value of {name}: {values.min():.4f} um={measure}')
    plt.title(f'{name} distribution') 
    plt.ylabel(f'Frequency, sum: {values.count()}')
    plt.xlabel(f'{name}, um={measure}')
    plt.legend()
    # Adjust layout to prevent clipping
    plt.tight_layout()

# rewrite for pandas
def get_feature_importances(model, train: pd.DataFrame) -> pd.DataFrame:
    '''Get feature importances from the model and sort them in descending order'''
    fi = pd.Series(model.feature_importances_)
    # Normalize feature importances
    feature_importances = [i / fi.sum() for i in fi]
    # add feature names
    features_cols = train.columns
    feature_importances = pd.DataFrame(
        list(zip(features_cols, feature_importances)),
        columns=["feature", "importance"]
    )
    # sort by importance
    feature_importances = feature_importances.sort_values(by="importance", ascending=False)
    
    return feature_importances



def generate_api_keys(num_keys=20, key_length=32, restrictions=None):
    """
    Generate API keys with optional restrictions.

    Args:
        num_keys (int): Number of keys to generate.
        key_length (int): Length of each API key.
        restrictions (dict): Dictionary of restrictions to assign to keys.

    Returns:
        dict: A dictionary of API keys and their associated restrictions.
    """
    keys = {}
    for _ in range(num_keys):
        # Generate a random API key
        key = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(key_length))
        
        # Assign restrictions if provided
        keys[key] = restrictions if restrictions else {}

    return keys

def save_keys_to_file(keys, filename="api_keys.json"):
    """
    Save API keys to a file in JSON format.

    Args:
        keys (dict): API keys and their restrictions.
        filename (str): File name to save the keys.
    """
    with open(filename, "w") as file:
        json.dump(keys, file, indent=4)
    print(f"API keys saved to {filename}")

# Example usage
if __name__ == "__main__":
    # Define restrictions (optional)
    restrictions_example = {
        "usage_limit": 1000,  # Max API calls
        "valid_until": "2025-12-31"  # Expiration date
    }

    # Generate 20 API keys with restrictions
    api_keys = generate_api_keys(num_keys=20, key_length=40, restrictions=restrictions_example)

    # Save to a file
    save_keys_to_file(api_keys)

def check_upd_dtypes(df_old: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
    """Check if the dtypes of the two dataframes are the same
        and if not, update the dtypes of the new dataframe to match the old one

    Args:
        df_old: df from the database
        df_new: df from the csv file
    """    
    dict_old = {}
    [dict_old.update({col: d_type}) for col, d_type in df_old.dtypes.items()]
    
    dict_new = {}
    [dict_new.update({col: d_type}) for col, d_type in df_new.dtypes.items()]
    
    # check if the dtypes are the same
    if dict_old != dict_new:
        # update dtypes of the new dataframe to match the old one
        for col, dtype in dict_old.items():
            if col in df_new.columns:
                df_new[col] = df_new[col].astype(dtype)
    
    return df_new

def update_df(df_new: pd.DataFrame, df_old: pd.DataFrame, key_column: str) -> pd.DataFrame:
    '''Update df_old from df_new (columns are equal) by key column and return new data and updated data'''
 
    # check and update dtypes in new df from old df
    df_new = check_upd_dtypes(df_old, df_new)
    
    # if old df is not empty
    if df_old.shape[0] > 0:

        # # Find the intersection of IDs between the two dataframes for find updated and new rows
        common_keys = pd.merge(df_new[[key_column]], df_old[[key_column]], on=key_column)[key_column]

        # find updated rows only
        updated_rows = []
        for key in common_keys:
            # get row with id from new and old dataframes
            row_new_tmp = df_new[df_new[key_column] == key] # use for adding
            row_new = row_new_tmp.squeeze() # use for comparing
            row_old = df_old[df_old[key_column] == key].squeeze()  # use for comparing

            # check if rows are different
            if not row_new.equals(row_old):
                # add to new_data dataframe
                updated_rows.append(row_new_tmp)

        # add new rows to updated_data dataframe or create empty dataframe if no new rows
        updated_data = pd.concat(updated_rows, ignore_index=True, axis='rows') if updated_rows else pd.DataFrame(columns=df_new.columns)
   
        # filter new data from new df
        new_data = df_new[~df_new[key_column].isin(common_keys)]
    
    else:
        # if old df is empty, return new df and empty updated df
        new_data = df_new.copy()
        updated_data = pd.DataFrame(columns=df_new.columns)

    return new_data, updated_data


def df_diff(df_new: pd.DataFrame, df_old: pd.DataFrame, key_column: str) -> pd.DataFrame:
    '''Find difference between two df by key column and return two df with new rows and updated rows'''  
    
    cols_in_new = set(df_new.columns)
    cols_in_old = set(df_old.columns)

    # check if columns are the same in both dataframes
    if cols_in_new == cols_in_old:
        new_data, updated_data = update_df(df_new, df_old, key_column)
                
    else:
        print(f"Columns {cols_in_new - cols_in_old} are not the same in new df and table from db")
        # set columns in new df to be the same as in old df
        df_new = df_new[df_old.columns.tolist()]

        new_data, updated_data = update_df(df_new, df_old, key_column)

    return new_data, updated_data

