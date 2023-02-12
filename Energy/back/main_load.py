"""
Files with functions to create sql database. 

Creation date: 16/01/2023
Last modification: 16/01/2023

for creating the sql db: 
https://datatofish.com/pandas-dataframe-to-sql/
"""
# required packages
import sqlite3

def create_db(df):
    """
    Function to create our db from an already transformed dataframe.
    
    :params df: pd.DataFrame
    : return None
    """
    # create sql database
    conn  = sqlite3.connect('../energy.db')
    c = conn.cursor()

    # create table
    c.execute("CREATE TABLE IF NOT EXISTS gazelec("
              "id INT NOT NULL PRIMARY KEY,"
              "date DATE,"
              "heure TIME,"
              "consommation_brute_gaz_grtgaz FLOAT UNSIGNED,"
              "consommation_brute_gaz_terega FLOAT UNSIGNED,"
              "consommation_brute_gaz_totale FLOAT UNSIGNED,"
              "consommation_brute_electricite_hph FLOAT UNSIGNED,"
              "consommation_brute_totale_hph FLOAT UNSIGNED," 
              "date_heure DATETIME NOT NULL)")
    conn.commit()

    # load data into df
    df.to_sql('gazelec', conn, if_exists='replace', index=False)


if __name__ == '__main__':
    print('You have run main_load.py file.')



