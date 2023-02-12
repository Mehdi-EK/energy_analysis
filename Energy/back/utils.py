"""
Preprocess data for analysis. In this file, the goal is to build a complete
set of tools to build a sql db in the main section. 

Creation date: 13/01/2023
Last modification: 16/01/2023

for creating the sql db: 
https://datatofish.com/pandas-dataframe-to-sql/
"""
# required packages
import pandas as pd 

# define functions for processing

def get_raw_data(file):
    """
    Function to download data from the csv file
    
    :params file: str
        path to csv file 
    :return raw_data: pd.DataFrame
        Dataframe containing data 
    """
    raw_data = pd.read_csv(file, sep=';')
    # change types of columns date and heure
    raw_data['date'] = pd.to_datetime(raw_data['date'], dayfirst=True)
    raw_data['heure'] = pd.to_datetime(raw_data['heure'], format='%H:%M') \
                        - pd.to_datetime(raw_data['heure'], format='%H:%M').dt.normalize()  
    return raw_data

def select_min_max_date(df, max_date=None, min_date=None): 
    """
    Function to drop rows from the dataframe after maximum date.
    
    :params df: pd.DataFrame
        This df is supposed to have the column 
    :params max_date: str
        Maximum date
    :params min_date: str
        Minimum date
    """
    if max_date is None:
        max_date = df.date.max()
    if min_date is None:
        min_date = df.date.min()
    return df.loc[(df.date >= min_date)
                  & (df.date <= max_date)]

def find_multiple_datetime(df): 
    """
    Function to find couple (date, time) that are more than once in the 
    dataset.
    
    :params df: pd.DataFrame
        df to investigate
    :return list
        list containing couples 
    """
    new_df = df.groupby(by=['date_heure']).count()
    dates = list(new_df.loc[(new_df.date >= 2) & (new_df.heure >= 2)].index)
    new_df_2 = df[['date_heure', 'date', 'heure']] \
                .loc[df.date_heure.isin(dates)] \
                .groupby(['date', 'heure']) \
                .count()
    new_dates = list(new_df_2.loc[new_df_2.date_heure >= 2].index)
    return new_dates

def find_missing_datetime(df): 
    """
    Function to find missing datetime in the dataframe
    
    :params df: pd.DataFrame
        pd.DataFrame to investigate
    :return list
    """
    new_df = df.groupby(by='date').count()
    dates = list(new_df.loc[new_df.heure < 48].index)
    return dates

def check_odd_even_indexes(df):
    """
    Function to check if odd (or even) indexes corresponds to round hours.
    In this case, the number of different values in odd or even position 
    should be 24.

    /!\ This function is not totaly correct due to the comment below.
    
    :params df: pd.DataFrame
        pd.DataFrame to investigate
    : return Boolean
        True if the df has the shape we want, else False
    """
    l_odd = pd.unique(df.loc[df.index % 2 == 1].heure)
    l_even = pd.unique(df.loc[df.index % 2 == 0].heure)
    if (len(l_odd) == 24) and (len(l_even) == 24): 
        return True  # Actually high proba that it is a yes but...
    return False 

def transform_hour_per_hour(df): 
    """
    Function to transform hour df into an hour per hour dataframe

    :params df: pd.DataFrame
        pd.DataFrame to transform 
    :return pd.DataFrame
        A pd.DataFrame hour per hour
    """
    if not check_odd_even_indexes(df):
        NotImplemented("The function is not implemented in this case yet"
                       " even or odd should correspond to round hours.")

    # transformation to apply to each row
    def transformation_1(row):
        if row.name % 2 == 1:
            return row.consommation_brute_electricite_rte
        i = row.name
        to_add = df.at[i+1, 'consommation_brute_electricite_rte'] 
        conso_hph = (row.consommation_brute_electricite_rte + to_add) / 2
        return conso_hph
    
    # apply transformation
    df['consommation_brute_electricite_rte_hph'] = df.apply(func=transformation_1, 
                                                            axis=1)

    # drop half of rows (useless for hour per hour dataset)
    labels_to_drop = [i for i in range(df.index.min(), df.index.max()+1)
                      if i % 2 == 1]
    df.drop(labels=labels_to_drop, axis=0, inplace=True)

    # second transformation to apply for updating total conso
    def transformation_2(row):
        gaz = row.consommation_brute_gaz_totale
        electricite = row.consommation_brute_electricite_rte_hph
        return gaz + electricite

    df['consommation_brute_totale_hph'] = df.apply(func=transformation_2, 
                                                   axis=1)
    
    return df  # return an hour per hour and updated dataframe
        

if __name__ == '__main__':
    print("Everything is ok")