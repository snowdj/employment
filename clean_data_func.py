import pandas as pd
import numpy as np
import itertools
import ipdb

# we start with df which has a single row for each person which contains their main and second jobs, so main and second sic, and main and second emptype (INECAC05 and SECJMBR) and a weighted count

# we need to sum together the main and second jobs. so we subset for mainemp, secondemp, mainsemp, secondsemp.

# for each subset, we add a sector column and create some new levels - each sector subet (including all dcms), totaluk (which is entire original subset), civil society, overlap. so we are left with a nice big data set with every combination of sic, sector, and region. 

# however, we need to join the 4 subsets together so that the main and second jobs counts can be added up. 

# currently we create some base columns (agg) to merge into so that each subset is aligned and can be added. We aggregate the subset by these columns, then left join into the data... 

# maybe it is best to not bother with the base columns and just outer join everything - and do the aggregating in the next stage - would need to make sure that sic, sector, region (not region, just cat, one of which is region!) do not have any missing values - for now include sic, sector and region in agg then try reducing after the for loop???



def expand_grid(data_dict):
   rows = itertools.product(*data_dict.values())
   return pd.DataFrame.from_records(rows, columns=data_dict.keys())

def clean_data(cat, df, sic_mappings, regionlookupdata, region, sic_level):
    #region = False
    
    if sic_level == True:
        level = 'sic'
    else:
        level = 'sector'
    
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
        if sic_level == True:
            agg = expand_grid(
               {'sic': np.unique(sic_mappings.sic),
               cat: np.unique(df[cat])
               }
            )
        else:
            agg = expand_grid(
               {'sector': x,
                #'sic': np.unique(sic_mappings.sic),
               cat: np.unique(df[cat])
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
        dftemp = df.copy()
        dftemp = dftemp[dftemp[emptype] == emptypeflag]
        # need separate sic column to allow merging - I think
        dftemp['sic'] = dftemp[sicvar]
        dftemp['count'] = dftemp['PWTA16']
        
        # total uk includes missing sics, so take copy before removing missing sics
        dftemp_totaluk = dftemp.copy()
        
        # remove rows from subset with missing sic
        dftemp = dftemp[np.isnan(dftemp.sic) == False]
        
        # add sector column and further subset to all sectors excluding all_dcms
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
        # reorder columns
        dftemp_totaluk = dftemp_totaluk[dftemp_sectors.columns.values]
        
        # append different subsets together
        dftemp = dftemp_totaluk.append(dftemp_sectors)
        dftemp = dftemp.append(dftemp_cs)
        dftemp = dftemp.append(dftemp_all_dcms)
        dftemp = dftemp.append(dftemp_all_dcms_overlap)
        dftemp['region'] = dftemp[regioncol]
        
        # groupby ignores NaN so giving NaNs a value
        dftemp['region'] = dftemp['region'].fillna('missing region')
        
        # this converts sic back to numeric
        dftemp = dftemp.infer_objects()
        
        # only total_uk sector has nan sics so groupby is dropping data - setting missing values to 'missing'
        dftemp['sic'] = dftemp['sic'].fillna(value=-1)
        
        # create column with unique name (which is why pd.DataFrame() syntax is used) which sums the count by sector
        aggtemp = pd.DataFrame({subset : dftemp.groupby( ['sector', cat, 'sic'])['count'].sum()}).reset_index()
        
        #if sic_level == False:
        #    aggtemp = pd.DataFrame({subset : aggtemp.groupby( ['sector', cat])[subset].sum()}).reset_index()
            #if region == False:
            #    aggtemp = aggtemp.groupby(['sector', cat])[subset].sum().reset_index()

        #else:
        #    aggtemp = pd.DataFrame({subset : aggtemp.groupby( ['sic', cat])[subset].sum()}).reset_index()
        
        # EXPECTING BELOW LINE TO HAVE SAME EFFECT AS REMOVING REGION FROM ABOVE AGGTEMP, BUT IT DOES NOT - THIS IS THE DEISCREPANCY THAT NEEDS INVESTIGATING.
                        
        # merge final stacked subset into empty dataset containing each sector and category level combo
        agg = pd.merge(agg, aggtemp, how='outer')
    
    # fill in missing values to avoid problems with NaN
    agg = agg.fillna(0)
    
    # reduce down to desired aggregate
    agg = agg.drop('sic', axis=1)
    agg = agg.groupby(['sector', cat]).sum()
    agg = agg.reset_index(['sector', cat])
    


    # sum main and second jobs counts together
    agg['emp'] = agg['mainemp'] + agg['secondemp']
    del agg['mainemp']
    del agg['secondemp']
    agg['selfemp'] = agg['mainselfemp'] + agg['secondselfemp']
    del agg['mainselfemp']
    del agg['secondselfemp']
    
    #import ipdb; ipdb.set_trace()
    
    # stack for emp, selfemp, total
    aggemp = agg[[level, cat, 'emp']]        
    aggemp = aggemp.rename(columns={'emp': 'count'})
    aggemp['emptype'] = 'employed'
    
    aggself = agg[[level, cat, 'selfemp']]
    aggself = aggself.rename(columns={'selfemp': 'count'})
    aggself['emptype'] = 'self employed'
    
    aggtotal = aggemp.copy()
    aggtotal['count'] = aggemp['count'] + aggself['count']
    aggtotal['emptype'] = 'total'
    
    aggfinal = aggemp.append(aggself)
    aggfinal = aggfinal.append(aggtotal)
    
    # add civil society and remove overlap from all_dcms
    if level == 'sector':
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
