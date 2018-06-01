import pandas as pd
import numpy as np
import itertools
import ipdb

# we start with df which has a single row for each person which contains their main and second jobs, so main and second sic, and main and second emptype (INECAC05 and SECJMBR) and a weighted count

# we need to sum together the main and second jobs. so we subset for mainemp, secondemp, mainsemp, secondsemp.

# for each subset, we add a sector column and create some new levels - each sector subet (including all dcms), totaluk (which is entire original subset), civil society, overlap. so we are left with a nice big data set with every combination of sic, sector, and cat (e.g. region). 

# however, we need to join the 4 subsets together so that the main and second jobs counts can be added up. 

# currently we create sector and cat base columns (agg) to outer join into (thereby adding sics) so that each subset is aligned and can be added. We 

# maybe it is best to not bother with the base columns and just outer join everything - and do the aggregating in the next stage - would need to make sure that sic, sector, region (not region, just cat, one of which is region!) do not have any missing values - for now include sic, sector and region in agg then try reducing after the for loop???



def expand_grid(data_dict):
   rows = itertools.product(*data_dict.values())
   return pd.DataFrame.from_records(rows, columns=data_dict.keys())

#@profile
def clean_data(df, table_params, sic_mappings, regionlookupdata, weightedcountcol):
    #region = False
    
    if table_params['mycat'] == 'region':
        catuniques = np.unique(regionlookupdata.mapno)
    else:
        catuniques = np.unique(df[table_params['mycat']])
    
    x = pd.Series(np.unique(sic_mappings.sector))
    y = pd.Series(["civil_society", "total_uk", "overlap"])
    x = x.append(y)
    
    agg = expand_grid(
       {'sector': x,
        #'sic': np.unique(sic_mappings.sic),
       table_params['mycat']: catuniques
       }
    )

    for subset in ['mainemp', 'secondemp', 'mainselfemp', 'secondselfemp']:
        if subset == 'mainemp':
            sicvar = "INDC07M"
            emptype = "INECAC05"
            emptypeflag = 1
            regioncol = 'regionmain'
    
        if subset == 'secondemp':
            sicvar = "INDC07S"
            emptype = "SECJMBR"
            emptypeflag = 1
            regioncol = 'regionsecond'
    
        if subset == 'mainselfemp':
            sicvar = "INDC07M"
            emptype = "INECAC05"
            emptypeflag = 2
            regioncol = 'regionmain'
    
        if subset == 'secondselfemp':
            sicvar = "INDC07S"
            emptype = "SECJMBR"
            emptypeflag = 2
            regioncol = 'regionsecond'
        
        # create subset for each of 4 groups
        df['region'] = df[regioncol]
        df['region'] = df['region'].fillna('missing region')
        dftemp = df[[sicvar, emptype, weightedcountcol, 'cs_flag', table_params['mycat']]].copy()
        dftemp = dftemp[dftemp[emptype] == emptypeflag]
        # need separate sic column to allow merging - I think
        dftemp.rename(columns={sicvar : 'sic'}, inplace=True)

        # total uk includes missing sics, so take copy before removing missing sics
        dftemp_totaluk = dftemp.copy()
        
        # remove rows from subset with missing sic
        dftemp = dftemp[np.isnan(dftemp.sic) == False]
        
        # add sector column and further subset to all sectors excluding all_dcms
        dftemp_sectors = pd.merge(dftemp, sic_mappings.loc[:,['sic', 'sector']], how = 'inner')
        dftemp_sectors = dftemp_sectors[dftemp_sectors['sector'] != 'all_dcms']
        
        # subset civil society
        dftemp_cs = dftemp[dftemp['cs_flag'] == 1]
        dftemp_cs['sector'] = 'civil_society'
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
        # reorder columns
        dftemp_totaluk = dftemp_totaluk[dftemp_sectors.columns.values]
        
        # append different subsets together
        dftemp = dftemp_totaluk.append(dftemp_sectors)
        dftemp = dftemp.append(dftemp_cs)
        dftemp = dftemp.append(dftemp_all_dcms)
        dftemp = dftemp.append(dftemp_all_dcms_overlap)
        
        # groupby ignores NaN so giving region NaNs a value
        
        # this converts sic back to numeric
        dftemp = dftemp.infer_objects()
        
        # only total_uk sector has nan sics so groupby is dropping data - setting missing values to 'missing'
        dftemp['sic'] = dftemp['sic'].fillna(value=-1)
        
        # create column with unique name (which is why pd.DataFrame() syntax is used) which sums the count by sector
        aggtemp = pd.DataFrame({subset : dftemp.groupby( ['sector', table_params['mycat'], 'sic'])[weightedcountcol].sum()}).reset_index()
        
        #if sic_level == False:
        #    aggtemp = pd.DataFrame({subset : aggtemp.groupby( ['sector', cat])[subset].sum()}).reset_index()
            #if region == False:
            #    aggtemp = aggtemp.groupby(['sector', cat])[subset].sum().reset_index()

        #else:
        #    aggtemp = pd.DataFrame({subset : aggtemp.groupby( ['sic', cat])[subset].sum()}).reset_index()
        
        # EXPECTING BELOW LINE TO HAVE SAME EFFECT AS REMOVING REGION FROM ABOVE AGGTEMP, BUT IT DOES NOT - THIS IS THE DEISCREPANCY THAT NEEDS INVESTIGATING.
                        
        # merge final stacked subset into empty dataset containing each sector and category level combo
        # should be able to just use aggtemp for first agg where subset=='mainemp', but gave error, need to have play around. checking that agg has all the correct sectors and cat levels should be a separate piece of code.
        agg = pd.merge(agg, aggtemp, how='outer')
        
    return agg
