import pandas as pd
import numpy as np
import itertools
import pdb

#aggfinal.loc[aggfinal['count'] < 6000, 'count'] = 0
#aggfinal['count'] = round(aggfinal['count'] / 1000, 0).astype(int)

# summary table
def region_summary_table(aggfinal, cat, perc, cattotal, catorder, region):

    # create two way table for regions
    for emptype in ['employed', 'self employed', 'total']:
        
        # subset data
        aggfinaltemp = aggfinal[aggfinal['emptype'] == emptype]
        aggfinaltemp = aggfinaltemp[aggfinaltemp['sector'] == 'all_dcms']
        aggfinaltemp.rename(columns={'count': emptype}, inplace=True)

        # create two way table
        emptable = aggfinaltemp[['region', emptype]].set_index(['region']).sort_index()
        emptable = emptable.groupby(['region']).sum()
        #emptable.columns = emptable.columns.droplevel(0)

        try:
            final = pd.concat([final, emptable], axis=1)
        except:
            final = emptable

        # we don't want to anonymise overlap !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # need to calculate percs before anonymising!!!! because anonymising 1 cat level will change the calculated perc of non anonymised cat levels. which means we have to anonymise the percs also.

    # add all regions row - NB: should include 'Outside UK' region level
    final.loc['All regions'] = final.sum()

    # remove outside uk and reorder
    myroworder = ['North East', 'North West', 'Yorkshire and the Humber', 'East Midlands', 'West Midlands', 'East of England', 'London', 'South East', 'South West', 'Wales', 'Scotland', 'Northern Ireland', 'All regions']
    final = final.reindex(myroworder)

    # add all uk row ==========================================================
    # 'All UK' is the sum of sum of sector='totaluk' excluding region='Outside UK'
    # in the publication, these numbers are the same across different tables which I have already matched - so should be able to recreate
    
    # create emp, semp, total columns
    totaluk = aggfinal.copy()
    totaluk = totaluk.set_index(['sector', 'region', 'emptype'])
    totaluk = totaluk.unstack()
    
    # remove outside uk
    #sumlevels = totaluk.index.levels[1].values.tolist()
    #sumlevels.remove('Outside UK')

    # drop hierarchical index and column names
    subdf = totaluk.loc[('total_uk', slice(None)), ]
    subdf = subdf.reset_index(level='sector', drop=True)
    subdf.columns = subdf.columns.droplevel(0)

    # append row
    final.loc['All UK'] = subdf.sum()    

    for emptype in ['employed', 'self employed']:
        # add perc columns
        if perc:
            final.loc[:, (emptype + '_perc')] = final[emptype] / final['total'] * 100

    # must anonymise after calculating perc
    mask = final.loc[:, ['employed', 'self employed', 'total']] < 6000
    final[mask] = 0

    # final.loc['Wales', 'employed'] = 0
    # final.loc['North East', 'self employed'] = 0

    # rounding
    final.loc[:, ['employed', 'self employed', 'total']] = round(final.loc[:, ['employed', 'self employed', 'total']] / 1000, 0).astype(int)
    
    # anonymise percs if corresponding counts are 0
    # create mask for emptype = 0 cases
    for empy in ['employed', 'self employed']:
        percmask = final.loc[:, empy] == 0
        final[empy + '_perc'][percmask] = 0
    
    # replace NaNs with 0
    final = final.fillna(0)

    # reorder columns
    mycolorder = ['employed', 'employed_perc', 'self employed', 'self employed_perc', 'total']
    final = final.reindex(columns=mycolorder)

    return final
    