import pandas as pd
import numpy as np
import itertools
import pdb

#aggfinal.loc[aggfinal['count'] < 6000, 'count'] = 0
#aggfinal['count'] = round(aggfinal['count'] / 1000, 0).astype(int)

# summary table
def summary_table(aggfinal, cat, perc, cattotal, catorder):
    
    for emptype in aggfinal['emptype'].unique():
        
        aggfinaltemp = aggfinal[aggfinal['emptype'] == emptype]
        aggfinaltemp.rename(columns={'count': emptype}, inplace=True)
        emptable = aggfinaltemp[['sector', emptype, cat]].set_index([cat, 'sector']).sort_index().unstack(cat)
        #emptable.columns = emptable.columns.droplevel(0)
        total = emptable.sum(axis=1)
        total.name = 'Total'
        
        emptable = emptable.reindex(level=1, columns=catorder)
        
        # emptype total
        if cattotal:
            emptable.loc[:, (emptype, 'Total')] = total
            #emptable = pd.concat([emptable, total], axis=1)
        
        # we don't want to anonymise overlap !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # need to calculate percs before anonymising!!!! because anonymising 1 cat level will change the calculated perc of non anonymised cat levels. which means we have to anonymise the percs also.
        colsforrounding = emptable.columns.levels[1]
        
        #pdb.set_trace()
        empnames = []
        if cattotal:
            percitercols = emptable.columns.levels[1][:-1]
        else:
            percitercols = emptable.columns.levels[1]
        
        # anonymise here

        # if emptype == 'self employed':
        #     pdb.set_trace()
        #emptable[emptable < 6000] = 0
        #emptable = round(emptable / 1000, 0).astype(int)
        
        if perc:
            for mycol in percitercols:
                emptable.loc[:, (emptype, mycol + '_perc')] = emptable[emptype][mycol] / total * 100
                empnames.append(mycol)
                empnames.append(mycol + '_perc')
            if cattotal:
                empnames.append('Total')
            emptable = emptable.reindex(level=1, columns=empnames)
        
        mask = emptable.loc[:, (slice(None), colsforrounding)] < 6000
        emptable[mask] = 0
        
        # anonymise percs if corresponding counts are 0
        if perc:
            catorderperc = [i + '_perc' for i in catorder]
            mask3 = emptable.loc[:, (slice(None), catorderperc)]
            for mycoly in catorder:
                mask3.loc[:, (slice(None), mycoly + '_perc')] = mask.loc[:, (slice(None), mycoly)].values
            
            emptable[mask3] = 0
        
        emptable.loc[:, (slice(None), colsforrounding)] = round(emptable.loc[:, (slice(None), colsforrounding)] / 1000, 0).astype(int)
        
        
        if 'final' in dir():
            final = pd.concat([final, emptable], axis=1)
        else:
            final = emptable.copy()
    
    # replace NaNs with 0
    final = final.fillna(0)
            
    # reorder rows
    myroworder = ["civil_society", "creative", "culture", "digital", "gambling", "sport", "telecoms", "all_dcms", "total_uk"]
    final = final.reindex(myroworder)
    return final
