"""
Main file to run to build the sql database. 

Creation date: 16/01/2023
Last modification: 16/01/2023

for creating the sql db: 
https://datatofish.com/pandas-dataframe-to-sql/
"""
# required packages
import os 

from main_transform import first_process
from main_load import create_db 

# define paths
base_path = os.getcwd()
file_path = os.path.join(base_path, 
                         '..', '..', 
                         'raw_daily_conso.csv')

# build db 
def main_0():
    """
    Function to build the sql database and create the first 
    table gazelec in it.
    """
    data = first_process(file_path)
    create_db(data)


if __name__ == '__main__':
    main_0()