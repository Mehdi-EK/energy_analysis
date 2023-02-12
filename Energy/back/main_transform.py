"""
File containing functions to preprocess data before creating
the sql database.

Creation date: 13/01/2023
Last modification: 16/01/2023

for creating the sql db: 
https://datatofish.com/pandas-dataframe-to-sql/
"""

# packages required
import os 
import pandas as pd 
from utils import get_raw_data, select_min_max_date, transform_hour_per_hour

# define paths
base_path = os.getcwd()
file_path = os.path.join(base_path, 
                         '..', '..', 
                         'raw_daily_conso.csv')

# functions for building db 

def first_process(path):
    """
    Main function to do the first preprocessing. This function 
    build the sql db with sqlite.
    
    :params path: str
        Path to the CSV file
    :return pd.DataFrame
        Transformed dataframùe for building our db 
    """
    # download & select data from the csv file
    data = get_raw_data(path)
    data = select_min_max_date(df=data, 
                               max_date='2022-05-31 00:00:00')
    data.drop(columns=['date_heure'], inplace=True) 

    # notice that row '05/012/2020 - 00:0' is present twice : fix
    data.at[36334, 'date'] = '2020-12-06'  #'06/12/2020'

    # sort data 
    data.sort_values(by=['date', 'heure'], inplace=True)
    data.reset_index(drop=True, inplace=True)

    # transform our df into an hour per hour df & reset index
    data = transform_hour_per_hour(data)
    data.reset_index(drop=True, inplace=True)

    # drop useless columns 
    columns_to_drop = ['consommation_brute_electricite_rte', 
                       'consommation_brute_totale', 
                       'statut_terega', 
                       'statut_grtgaz', 
                       'statut_rte']
    data.drop(columns=columns_to_drop, inplace=True)

    # creta a primary key
    data['date_heure'] = pd.to_datetime(data['date'] + data['heure'], 
                                        format='%Y-%m-%d:%M:%S')

    return data

def process_0(path): 
    """
    Function to use in the future for adding rows to the table 
    gazelec.
    
    :params path: str
        Path to the csv file
    :return pd.DataFrame
        Transformed dataframe"""
    pass
    

if __name__ == '__main__':
    print("You have run main_transform.py")
    data = first_process(file_path)
    print(data[['date_heure']].tail(10))
    print(data.columns)