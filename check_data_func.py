import pandas as pd
import numpy as np
import itertools

from openpyxl import load_workbook, Workbook
from openpyxl.utils.dataframe import dataframe_to_rows    

def check_data(data, wsname, startrow, startcol, finishrow, finishcol):
    # check data against publication
    
    # read in publication data    
    wb = load_workbook('DCMS_Sectors_Economic_Estimates_Employment_2016_tables_2.xlsx')
    ws = wb[wsname]
    
    exceldata = ws.values
    exceldata = list(exceldata)
    newdata = []
    
    # copy data into list
    for row in range(startrow - 1, finishrow):
        listrow = list(exceldata[row][(startcol-1):finishcol])
        
        # code anonymised as 0s
        listrow = [0 if x == '-' else x for x in listrow]
        
        # code NA as 999999s
        listrow = [999999 if x == 'N/A' else x for x in listrow]
        
        listrow = [-999999 if pd.isnull(x) else x for x in listrow]

        newdata.append(listrow)
    
    exceldataframe = pd.DataFrame(newdata, index=data.index, columns=data.columns)
    exceldataframe.dtypes
    return exceldataframe
