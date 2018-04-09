import pandas as pd
import numpy as np
import itertools
import ipdb



def expand_grid(data_dict):
   rows = itertools.product(*data_dict.values())
   return pd.DataFrame.from_records(rows, columns=data_dict.keys())

def clean_data(cat, df, sic_mappings, regionlookupdata, region):
    #region = False
    
    x = pd.Series(np.unique(sic_mappings.sector))
    y = pd.Series(["civil_society", "total_uk", "overlap"])
    x = x.append(y)
    
    if region == True:
        agg = expand_grid(
           {'sector': x,
           'region': np.unique(regionlookupdata.mapno)
           }
        )
    else:
        agg = expand_grid(
           {'sector': x,
           cat: np.unique(df[cat])
           }
        )

    for i in range(1,5):
        if i == 1:
            sicvar = "INDC07M"
            emptype = "INECAC05"
            emptypeflag = 1
            countname = "mainemp"
            regioncol = 'regionmain'
    
        if i == 2:
            sicvar = "INDC07S"
            emptype = "SECJMBR"
            emptypeflag = 1
            countname = "secondemp"
            regioncol = 'regionsecond'
    
        if i == 3:
            sicvar = "INDC07M"
            emptype = "INECAC05"
            emptypeflag = 2
            countname = "mainselfemp"
            regioncol = 'regionmain'
    
        if i == 4:
            sicvar = "INDC07S"
            emptype = "SECJMBR"
            emptypeflag = 2
            countname = "secondselfemp"
            regioncol = 'regionsecond'
        
        # base subset for each of 4 groups
        dftemp = df.copy()
        dftemp = dftemp[dftemp[emptype] == emptypeflag]
        dftemp['sic'] = dftemp[sicvar]
        dftemp['count'] = dftemp['PWTA16']
        
        dftemp_totaluk = dftemp.copy()
        
        dftemp = dftemp[np.isnan(dftemp.sic) == False]
        
        # subset for sectors excluding all_dcms
        dftemp_sectors = pd.merge(dftemp, sic_mappings.loc[:,['sic', 'sector']], how = 'inner')
        dftemp_sectors = dftemp_sectors[dftemp_sectors['sector'] != 'all_dcms']
        
        # subset civil society
        dftemp_cs = dftemp[dftemp['cs_flag'] == 1]
        dftemp_cs.loc[:, 'sector'] = 'civil_society'
        dftemp_cs = dftemp_cs[dftemp_sectors.columns.values]
        
        # subset all_dcms (still need to add cs and remove overlap)
        dftemp_all_dcms = pd.merge(dftemp, sic_mappings.loc[:,['sic', 'sector']], how = 'inner')
        dftemp_all_dcms = dftemp_all_dcms[dftemp_all_dcms['sector'] == 'all_dcms']
        
        # subset overlap between sectors
        dftemp_all_dcms_overlap = pd.merge(dftemp, sic_mappings.loc[:,['sic', 'sector']], how = 'inner')
        dftemp_all_dcms_overlap = dftemp_all_dcms_overlap[dftemp_all_dcms_overlap['sector'] == 'all_dcms']
        dftemp_all_dcms_overlap = dftemp_all_dcms_overlap[dftemp_all_dcms_overlap['cs_flag'] == 1]
        dftemp_all_dcms_overlap['sector'] = 'overlap'
        
        # subset uk total
        dftemp_totaluk['sector'] = 'total_uk'
        dftemp_totaluk = dftemp_totaluk[dftemp_sectors.columns.values]
        
        # append different subsets together
        dftemp = dftemp_totaluk.append(dftemp_sectors)
        dftemp = dftemp.append(dftemp_cs)
        dftemp = dftemp.append(dftemp_all_dcms)
        dftemp = dftemp.append(dftemp_all_dcms_overlap)
        dftemp['region'] = dftemp[regioncol]
        
        # groupby ignores NaN so giving NaNs a value
        dftemp['region'] = dftemp['region'].fillna('missing region')
        
        # create column with unique name which sums the count by sector
        aggtemp = pd.DataFrame({countname :
        dftemp.groupby( ['sector', cat]
                  )['count'].sum()}).reset_index()
        
        # EXPECTING BELOW LINE TO HAVE SAME EFFECT AS REMOVING REGION FROM ABOVE AGGTEMP, BUT IT DOES NOT - THIS IS THE DEISCREPANCY THAT NEEDS INVESTIGATING.
        
        #ipdb.set_trace()
        if region == False:
            aggtemp = aggtemp.groupby(['sector', cat])[countname].sum().reset_index()
        #import ipdb; ipdb.set_trace()
        
        # merge final stacked subset into empty dataset containing each sector and category level combo 
        agg = pd.merge(agg, aggtemp, how='left')
    
    # sum main and second jobs counts together
    agg = agg.fillna(0)
    agg['emp'] = agg['mainemp'] + agg['secondemp']
    del agg['mainemp']
    del agg['secondemp']
    agg['selfemp'] = agg['mainselfemp'] + agg['secondselfemp']
    del agg['mainselfemp']
    del agg['secondselfemp']
    
    #import ipdb; ipdb.set_trace()
    
    # stack for emp, selfemp, total
    aggemp = agg[['sector', cat, 'emp']]        
    aggemp = aggemp.rename(columns={'emp': 'count'})
    aggemp['emptype'] = 'employed'
    
    aggself = agg[['sector', cat, 'selfemp']]
    aggself = aggself.rename(columns={'selfemp': 'count'})
    aggself['emptype'] = 'self employed'
    
    aggtotal = aggemp.copy()
    aggtotal['count'] = aggemp['count'] + aggself['count']
    aggtotal['emptype'] = 'total'
    
    aggfinal = aggemp.append(aggself)
    aggfinal = aggfinal.append(aggtotal)
    
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
        print(cat + ': missing values')
    else:
        print(cat + ': no missing values')
    
    # anonymise individual elements - messes up cat totals if done here
    #aggfinal.loc[aggfinal['count'] < 6000, 'count'] = 0
    
    # format values - again should does this after cat totals
    #aggfinal['count'] = round(aggfinal['count'] / 1000, 0).astype(int)
    return aggfinal
