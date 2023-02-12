"""
This file aims to extract data using an API. 
Data at: 
https://odre.opendatasoft.com/explore/dataset/eco2mix-national-tr/api/?disjunctive.nature&lang=fr

Creation: 25/01/2023
Last modification: 26/01/2023
"""

import requests 
import pandas as pd 

from utils import build_url_0, request_to_get_data_0, build_dataframe_0

#url_0 = "https://odre.opendatasoft.com/api/records/1.0/search/?dataset=eco2mix-national-tr&q=&lang=fr&facet=nature&facet=date_heure"
# url_1 = "https://odre.opendatasoft.com/api/records/1.0/search/?dataset=eco2mix-national-tr&q=date_heure%3A%5B2022-12-31T23%3A00%3A00Z+TO+2023-01-25T22%3A59%3A59Z%5D&lang=fr&rows=3000&sort=-date_heure&facet=nature&facet=date_heure"


def main_0(url):
    r = requests.get(url)
    json_file = r.json()

    data = json_file['records']

    new_data = {}
    for row, value in enumerate(data):
        new_data[str(row)] = value['fields']
 
    df = pd.DataFrame.from_dict(new_data, orient='index')
    df.drop(columns=['date_heure'])

    df['date'] = pd.to_datetime(df['date'], dayfirst=True)
    df['heure'] = pd.to_datetime(df['heure'], format='%H:%M') \
                  - pd.to_datetime(df['heure'], format='%H:%M').dt.normalize()
    
    df['date_heure'] = pd.to_datetime(df['date'] + df['heure'], 
                                        format='%Y-%m-%d:%M:%S')
    
    print(df.dtypes)
    
    df.sort_values(by='date_heure', inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    col_pr = ['date_heure', 'solaire', 'nucleaire', 'eolien', 'consommation']
    print(df[col_pr].describe())
    print(df[col_pr].tail())
    

    return None


def main_1(start_date, end_date, rows=3000):
    """
    Note that here
    start_date is not included and end_date is included in the interval.

    :params start_date: str 
        Start date (not included). Format year-month-day.
    :params end_date: str  
        end date (included). Format year-month-day.
    :params rows: int 
        Maximum number of rows to grab. 
    : return pandas.DataFrame
    """
    url = build_url_0(start_date, end_date, rows)
    raw_data = request_to_get_data_0(url)
    return build_dataframe_0(raw_data)



if __name__ == '__main__': 

    print("Check has started \n")

    df = main_1("2022-12-31", "2023-01-25")
    print(df.dtypes) 


