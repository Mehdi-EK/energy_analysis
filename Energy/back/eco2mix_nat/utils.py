"""
This file aims to contains function for extracting, transforming and loading data
from an API for eco2mix analysis.

Data at: 
https://odre.opendatasoft.com/explore/dataset/eco2mix-national-tr/api/?disjunctive.nature&lang=fr

Creation: 25/01/2023
Last modification: 26/01/2023
"""

import pandas as pd 
import requests


### Functions for EXTRACT 

def build_url_0(start, end, rows):
    """"
    Function to build url to use to grab data. 
    """
    url = f"https://odre.opendatasoft.com/api/records" \
        f"/1.0/search/?dataset=eco2mix-national-tr&q" \
        f"=date_heure%3A%5B{start}T23%3A00%3A00Z+TO+" \
        f"{end}T22%3A59%3A59Z%5D&lang=fr&rows={rows}&" \
        f"sort=-date_heure&facet=nature&facet=date_heure"
    return url 

def request_to_get_data_0(url):
    """
    Make request to get data

    :return list
    """
    r = requests.get(url)
    json_file = r.json()
    data = json_file['records']
    return data

def build_dataframe_0(data):
    """
    Build a pandas.DataFrame

    :params data: list
    :return pandas.DataFrame
    """
    # Initialization of my new dictionary 
    new_data = {}

    # now transform data into type:
    # Type {row0: {col0:val0, col1:val1}, row1:{col0:val0, ...}}
    for row, value in enumerate(data):
        new_data[str(row)] = value['fields']

    # orient index is from the structure
    df = pd.DataFrame.from_dict(new_data, orient='index')
    return df


    ### Functions for TRANSFORM

    def transform_type(df):
        """
        Function to change type of columns in the df.
        
        :params df: pandas.DataFrame
            dataframe contnaining eco2mix data.
        :return pandas.DataFrame    
        """
        # drop unusablee columns
        df.drop(columns=['date_heure'])

        # convert tpye of date and time columns
        df['date'] = pd.to_datetime(df['date'], dayfirst=True)
        df['heure'] = pd.to_datetime(df['heure'], format='%H:%M') \
                    - pd.to_datetime(df['heure'], format='%H:%M').dt.normalize()
        
        # recreate a good date_time column
        df['date_heure'] = pd.to_datetime(df['date'] + df['heure'], 
                                            format='%Y-%m-%d:%M:%S')

        # sort by dates and reset index        
        df.sort_values(by='date_heure', inplace=True)
        df.reset_index(drop=True, inplace=True)
        
        return df

