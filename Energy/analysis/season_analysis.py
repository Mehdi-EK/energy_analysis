"""
File containing functions to make an analysis of 
consumption during winter

Creation date: 22/01/2023
Last modification: 24/01/2023
"""
# import packages
import pandas as pd 
import sqlite3
import seaborn as sns 
import matplotlib.pyplot as plt 

def load_data(): 
    conn = sqlite3.connect('../energy.db')
    c = conn.cursor()

    c.execute("""
                SELECT *
                FROM gazelec
                WHERE (date BETWEEN '2012-12-01 00:00' AND '2013-03-01 00:00') OR
                      (date BETWEEN '2013-12-01 00:00' AND '2014-03-01 00:00') OR
                      (date BETWEEN '2014-12-01 00:00' AND '2015-03-01 00:00') OR
                      (date BETWEEN '2015-12-01 00:00' AND '2016-03-01 00:00') OR
                      (date BETWEEN '2016-12-01 00:00' AND '2017-03-01 00:00') OR
                      (date BETWEEN '2017-12-01 00:00' AND '2018-03-01 00:00') OR
                      (date BETWEEN '2018-12-01 00:00' AND '2019-03-01 00:00') OR
                      (date BETWEEN '2019-12-01 00:00' AND '2020-03-01 00:00') OR
                      (date BETWEEN '2020-12-01 00:00' AND '2021-03-01 00:00') OR
                      (date BETWEEN '2020-12-01 00:00' AND '2022-03-01 00:00')
              """)

    columns = ['date', 'heure', 'consommation_brute_gaz_grtgaz',
       'consommation_brute_gaz_terega', 'consommation_brute_gaz_totale',
       'consommation_brute_electricite_rte_hph',
       'consommation_brute_totale_hph', 'date_heure']

    df = pd.DataFrame(c.fetchall(), columns=columns)

    return df

def smart_load_data(min_year, max_year):
    """
    Function to load data interesting for anlaysing consumption during winters.
    In this function, a new column is added named 'winter' through the functin 
    winter to determine to which winter a row belongs to.
    """
    # name of different columns
    columns = ['date', 'heure', 'consommation_brute_gaz_grtgaz',
       'consommation_brute_gaz_terega', 'consommation_brute_gaz_totale',
       'consommation_brute_electricite_rte_hph',
       'consommation_brute_totale_hph', 'date_heure']
    
    # initialization of the df to return 
    main_df = None 

    for year in range(min_year, max_year+1):
        # second_df = select_data_from_winter(year, columns)
        second_df = select_data_from_winter_2(year)
        second_df = winter(second_df, year)
        main_df = merge_winter_df(main_df, second_df)

    return main_df


def winter(df, year):
    """Function to add a columnn winter to a dataframe.
    
    :params df: pd.DataFrame
        DataFrame we want to qadd a new column
    :params year: int
        Value with which we want to fill the column winter (same for all)
    :return pd.DataFrame
        The modified dataframe.
    """
    years = [str(year-1)+'-'+str(year) for _ in range(df.shape[0])]
    df['winter'] = years
    return df 

def merge_winter_df(df1, df2):
    """
    Function used to merge a second dataframe df2 into a main dataframe 
    df1. 

    :params df1: pd.DataFrame or None
        Main dataframe. If None return df2.
    :params df2: pd.DataFrame 
        Second dataframe. Can't be None
    :return pd.DataFrame
        Merged dataframe
    """
    # check i a ùmain dataframe already exists.
    if df1 is None: 
        return df2
    return pd.concat([df1, df2], ignore_index=True)

def select_data_from_winter(year, columns):
    """
    Function making a SQL request for getting data from one specific winter
    """
    min_year = year -1

    conn = sqlite3.connect('../energy.db')
    c = conn.cursor()

    c.execute(f"SELECT * FROM gazelec "
              f"WHERE (date BETWEEN '{min_year}-12-01 00:00' AND " 
              f"'{year}-03-01 00:00')" 
              )

    return pd.DataFrame(c.fetchall(), columns=columns)

def select_data_from_winter_2(year):
    """
    Function making a SQL request for getting data from one specific winter
    """
    min_year = year -1

    conn = sqlite3.connect('../energy.db')
    c = conn.cursor()

    c.execute(f"SELECT AVG(consommation_brute_gaz_totale), "
              f"AVG(consommation_brute_electricite_rte_hph), "
              f"AVG(consommation_brute_totale_hph), " 
              f"date " 
              f"FROM gazelec "
              f"WHERE (date BETWEEN '{min_year}-12-01 00:00' AND " 
              f"'{year}-03-01 00:00')" 
              f"GROUP BY  date"
              )
            
    new_columns = ['consommation_brute_gaz_totale', 
                'consommation_brute_electricite_rte_hph', 
                'consommation_brute_totale_hph', 
                'date']

    return pd.DataFrame(c.fetchall(), columns=new_columns)

def winter_visualization(df):
    """
    Function for observation of distribution of data.
    
    :params df: pd.DataFrame
        Dataframe containing data of several winter. 
    (output of smart load data function) 
    :return None
        Plot for visualizing data.
    """
    # initialization of the figure
    fig, ax = plt.subplots(2, 2, figsize=(10, 30))

    # plot the consumption with horizontal boxes
    sns.boxplot(ax=ax[0, 0], 
                x='consommation_brute_electricite_rte_hph', 
                y='winter', 
                data=df, 
                whis=[0, 100], 
                width=.6, 
                palette='vlag')

    sns.boxplot(ax=ax[0, 1], 
                x='consommation_brute_gaz_totale', 
                y='winter', 
                data=df, 
                whis=[0, 100], 
                width=.6, 
                palette='vlag')

    sns.boxplot(ax=ax[1, 0], 
                x='consommation_brute_totale_hph', 
                y='winter', 
                data=df, 
                whis=[0, 100], 
                width=.6, 
                palette='vlag')

    # visual presentation 
    ax[0, 0].set(ylabel='')
    ax[0, 0].set(xlabel='consommation brute (moyenne MW) electricite quotidienne (RTE) pendant l\'hiver')
    # ax[0].set_title('Distribution de la consommation électrique journalière par année')
    ax[0, 0].xaxis.grid(True)

    ax[0, 1].set(ylabel='')
    ax[0, 1].set(xlabel='consommation brute (moyenne MW) gaz quotidienne (teraga + grtgaz) pendant l\'hiver')
    # ax[1].set_title('Distribution de la consommation de gaz journalière par année')
    ax[0, 1].xaxis.grid(True)

    ax[1, 0].set(ylabel='')
    ax[1, 0].set(xlabel='consommation brute (moyenne MW) totale (gaz+electricite) quotidienne pendant l\'hiver')
    # ax[2].set_title('Distribution de la consommation totale (gaz+électricité) journalière par année')
    ax[1, 0].xaxis.grid(True)

    plt.show()


if __name__ == '__main__':
    """df = smart_load_data(2013, 2022)
    drop_col = list(set(df.columns) - set(['date', 'consommation_brute_electricite_rte_hph', 'winter']))
    df_2 = df.drop(columns=drop_col)
    df_2 = df_2.groupby(by=['winter', 'date']).mean()
    print(df_2.head(10))
    print(winter_visualization(df_2))"""

    df = smart_load_data(2013, 2022)
    print(winter_visualization(df))
