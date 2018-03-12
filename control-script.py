#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  7 12:22:31 2018

@author: max.unsted
"""

import pandas as pd
import numpy as np
import itertools

df = pd.read_csv("~/data/cleaned_2016_df.csv")

cat = 'sex'
df['cat'] = df[cat]

sic_mappings = pd.read_csv("~/projects/employment/sic_mappings.csv")
sic_mappings = sic_mappings[sic_mappings.sic != 62.011]
sic_mappings.sic = round(sic_mappings.sic * 100, 0)
sic_mappings.sic = sic_mappings.sic.astype(int)

def expand_grid(data_dict):
   rows = itertools.product(*data_dict.values())
   return pd.DataFrame.from_records(rows, columns=data_dict.keys())

x = pd.Series(np.unique(sic_mappings.sector))
y = pd.Series(["civil_society", "total_uk", "overlap"])
x = x.append(y)
agg = expand_grid(
   {'sector': x,
   'sex': np.unique(df.sex)}
)

for i in range(1,5):
    if i == 1:
        sicvar = "INDC07M"
        emptype = "INECAC05"
        emptypeflag = 1
        countname = "mainemp"

    if i == 2:
        sicvar = "INDC07S"
        emptype = "SECJMBR"
        emptypeflag = 1
        countname = "secondemp"

    if i == 3:
        sicvar = "INDC07M"
        emptype = "INECAC05"
        emptypeflag = 2
        countname = "mainselfemp"

    if i == 4:
        sicvar = "INDC07S"
        emptype = "SECJMBR"
        emptypeflag = 2
        countname = "secondselfemp"
    
    dftemp = df.copy()
    dftemp = dftemp[dftemp[emptype] == emptypeflag]
    dftemp['sic'] = dftemp[sicvar]
    dftemp['count'] = dftemp['PWTA16']
    
    dftemp_totaluk = dftemp.copy()
    
    dftemp = dftemp[np.isnan(dftemp.sic) == False]
    
    dftemp_sectors = pd.merge(dftemp, sic_mappings.loc[:,['sic', 'sector']], how = 'inner')
    dftemp_sectors = dftemp_sectors[dftemp_sectors['sector'] != 'all_dcms']
    
    dftemp_cs = dftemp[dftemp['cs_flag'] == 1]
    dftemp_cs.loc[:, 'sector'] = 'civil_society'
    dftemp_cs = dftemp_cs[dftemp_sectors.columns.values]
    
    dftemp_all_dcms = pd.merge(dftemp, sic_mappings.loc[:,['sic', 'sector']], how = 'inner')
    dftemp_all_dcms = dftemp_all_dcms[dftemp_all_dcms['sector'] == 'all_dcms']
    
    dftemp_all_dcms_overlap = pd.merge(dftemp, sic_mappings.loc[:,['sic', 'sector']], how = 'inner')
    dftemp_all_dcms_overlap = dftemp_all_dcms_overlap[dftemp_all_dcms_overlap['sector'] == 'all_dcms']
    dftemp_all_dcms_overlap = dftemp_all_dcms_overlap[dftemp_all_dcms_overlap['cs_flag'] == 1]
    dftemp_all_dcms_overlap['sector'] = 'overlap'
    
    dftemp_totaluk['sector'] = 'total_uk'
    dftemp_totaluk = dftemp_totaluk[dftemp_sectors.columns.values]
    
    dftemp = dftemp_totaluk.append(dftemp_sectors)
    dftemp = dftemp.append(dftemp_cs)
    dftemp = dftemp.append(dftemp_all_dcms)
    dftemp = dftemp.append(dftemp_all_dcms_overlap)
    
    aggtemp = pd.DataFrame({countname :
    dftemp.groupby( ['sector',cat]
              )['count'].sum()}).reset_index()
    
    agg = pd.merge(agg, aggtemp, how='left')

agg = agg.fillna(0)
agg['emp'] = agg['mainemp'] + agg['secondemp']
del agg['mainemp']
del agg['secondemp']
agg['selfemp'] = agg['mainselfemp'] + agg['secondselfemp']
del agg['mainselfemp']
del agg['secondselfemp']

# stack empytype
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
    print('missing values')
else:
    print('no missing values')

# anonymise individual elements
aggfinal.loc[aggfinal['count'] < 6000, 'count'] = 0

# format values
aggfinal['count'] = round(aggfinal['count'] / 1000, 0).astype(int)

# summary table
if 'final' in globals():
    del final

perc = True
cattotal = True
catorder = ['Male', 'Female']

for emptype in aggfinal['emptype'].unique():
    
    aggfinaltemp = aggfinal[aggfinal['emptype'] == emptype]
    emptable = aggfinaltemp[['sector', 'count', 'sex']].set_index(['sex', 'sector']).sort_index().unstack('sex')
    emptable.columns = emptable.columns.droplevel(0)
    total = emptable.sum(axis=1)
    total.name = 'Total'
    emptable = emptable[catorder]
    
    empnames = []
    if perc:
        for mycol in emptable.columns:
            emptable[mycol + '_perc'] = emptable[mycol] / total * 100
            empnames.append(mycol)
            empnames.append(mycol + '_perc')
        emptable = emptable[empnames]
    
    # emptype total
    if cattotal:
        emptable = pd.concat([emptable, total], axis=1)
    
    if 'final' in globals():
        final = pd.concat([final, emptable], axis=1)
    else:
        final = emptable.copy()

# reorder rows
myroworder = ["civil_society", "creative", "culture", "digital", "gambling", "sport", "telecoms", "all_dcms", "total_uk"]
final = final.reindex(myroworder)

final.to_csv('test.csv')

# get final table with hierarchical indexes which I can check against those read in from excel (including order of rows etc), but then just output the values to the formatted excel templates

# for anonymisation, it seems something quite simple will work initially. Only gender, age, and nnsec seem like they will require rules
















































