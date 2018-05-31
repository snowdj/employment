import pandas as pd
import numpy as np
import itertools
import ipdb

def aggregate_data(agg, table_params):

    
    # fill in missing values to avoid problems with NaN
    agg = agg.fillna(0)
    
    # sum main and second jobs counts together
    agg['emp'] = agg['mainemp'] + agg['secondemp']
    del agg['mainemp']
    del agg['secondemp']
    agg['selfemp'] = agg['mainselfemp'] + agg['secondselfemp']
    del agg['mainselfemp']
    del agg['secondselfemp']
    
    #import ipdb; ipdb.set_trace()
    
    # stack for emp, selfemp, total
    aggemp = agg[['sector', 'sic', table_params['mycat'], 'emp']]        
    aggemp = aggemp.rename(columns={'emp': 'count'})
    aggemp['emptype'] = 'employed'
    
    aggself = agg[['sector', 'sic', table_params['mycat'], 'selfemp']]
    aggself = aggself.rename(columns={'selfemp': 'count'})
    aggself['emptype'] = 'self employed'
    
    aggtotal = aggemp.copy()
    aggtotal['count'] = aggemp['count'] + aggself['count']
    aggtotal['emptype'] = 'total'
    
    aggfinal = aggemp.append(aggself)
    aggfinal = aggfinal.append(aggtotal)
    
    # reduce down to desired aggregate
    aggfinal = aggfinal.drop('sic', axis=1)
    aggfinal = aggfinal.groupby(['sector', table_params['mycat'], 'emptype']).sum()
    aggfinal = aggfinal.reset_index(['sector', table_params['mycat'], 'emptype'])
    
    # add civil society and remove overlap from all_dcms
    aggfinaloverlap = aggfinal.copy()
    aggfinaloverlap = aggfinaloverlap.reset_index(drop=True)
    
    alldcmsindex = aggfinaloverlap[aggfinaloverlap['sector'] == 'all_dcms'].index
    csindex = aggfinaloverlap[aggfinaloverlap['sector'] == 'civil_society'].index
    overlapindex = aggfinaloverlap[aggfinaloverlap['sector'] == 'overlap'].index
    newalldcms = aggfinaloverlap.loc[alldcmsindex, ['count']].reset_index(drop=True) + aggfinaloverlap.loc[csindex, ['count']].reset_index(drop=True) - aggfinaloverlap.loc[overlapindex, ['count']].reset_index(drop=True)
    newalldcms2 = newalldcms['count']
    newalldcms3 = np.array(newalldcms2)
    aggfinaloverlap.loc[alldcmsindex, ['count']] = newalldcms3
    
    aggfinal = aggfinaloverlap.copy()
    
    # drop tourism
    aggfinal = aggfinal[aggfinal['sector'] != 'tourism'] # this is redundant
    # check for any missing values

    if aggfinal.isnull().values.any() == True:
        print(table_params['mycat'] + ': missing values')
    else:
        print(table_params['mycat'] + ': no missing values')
    
    # anonymise individual elements - messes up cat totals if done here
    #aggfinal.loc[aggfinal['count'] < 6000, 'count'] = 0
    
    # format values - again should does this after cat totals
    #aggfinal['count'] = round(aggfinal['count'] / 1000, 0).astype(int)
    return aggfinal
