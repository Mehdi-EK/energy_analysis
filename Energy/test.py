import sqlite3
import pandas as pd 

def main():
    conn = sqlite3.connect('./energy.db')
    c = conn.cursor()

    c.execute("""
                SELECT * 
                FROM gazelec
                WHERE date BETWEEN '2020-05-05 00:00' AND '2020-06-05 00:00'
              """)
    # c.execute(""" 
    #            SELECT max(consommation_brute_gaz_totale) FROM gazelec
    #           """)

    columns = ['date', 'heure', 'consommation_brute_gaz_grtgaz',
       'consommation_brute_gaz_terega', 'consommation_brute_gaz_totale',
       'consommation_brute_electricite_rte_hph',
       'consommation_brute_totale_hph', 'date_heure']
    columns_2 = ['consommation_brute_gaz_totale', 
                'consommation_brute_electricite_rte_hph', 
                'consommation_brute_totale_hph', 
                'date']
    # df = pd.DataFrame(c.fetchall(), columns=columns_2)
    print(c.fetchall())
    print(type(c.fetchall()))

def drop_table(nom_table):
    """
    Function to drop a table using its name.
    """
    conn = sqlite3.connect('./energy.db')
    c = conn.cursor()

    c.execute(f"DROP TABLE IF EXISTS {nom_table}")
    conn.commit()


if __name__ == '__main__': 
    main()
    # drop_table()