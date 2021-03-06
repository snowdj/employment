import pandas as pd
import numpy as np
import itertools

def anonymise(data, emptypecats, anoncats, cat, sector):
    # anonymisation
    
    if cat == 'region':
        if sector != 'gambling' and sector != 'telecoms':
            for row in data.index:
                for emptype in ['employed', 'self employed']:
                    if data[emptype][row] == 0:
                        otheremptype = ['employed', 'self employed']
                        otheremptype.remove(emptype)
                        data.loc[row, otheremptype[0]] = 0
                        data.loc[row, otheremptype[0] + '_perc'] = 0
    
    else:
        cat1 = data.columns.levels[0].values.tolist()
        #cat2 = ['Male', 'Male_perc', 'Female', 'Female_perc', 'Total']
        cat2 = data.columns.levels[1].values.tolist()

        # if emp is zero, set semp to 0 and vice versa
        if emptypecats == True:
            for row in data.index:
                for emptype in cat1[:-1]:
                    for sex in cat2:
                        if data[emptype][sex][row] == 0:
                            otheremptype = cat1[:-1]
                            otheremptype.remove(emptype)
                            data.loc[row, (otheremptype[0], sex)] = 0
        
        # make sure there is not only 1 zero in non total cat levels
        for row in data.index:
            for emptype in cat1:
                for sex in cat2[:-1]:
                    if anoncats == []:
                        if sum(data.loc[row, emptype].isin([0])) == 1:
                            if data.loc[row, (emptype, cat2[0])] == 0:
                                data.loc[row, (emptype, cat2[1])] = 0
                            else:
                                data.loc[row, (emptype, cat2[0])] = 0
                    else:
                        if sum(data.loc[row, emptype].isin([0])) == 1:
                            if data.loc[row, (emptype, anoncats[0])] == 0:
                                data.loc[row, (emptype, anoncats[1])] = 0
                            elif data.loc[row, (emptype, anoncats[1])] == 0:
                                data.loc[row, (emptype, anoncats[0])] = 0
                            else:
                                data.loc[row, (emptype, anoncats[0])] = 0
    
    return data
