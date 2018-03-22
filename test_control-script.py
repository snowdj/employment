import pandas as pd
import numpy as np
import itertools

df = pd.read_csv("~/data/cleaned_2016_df.csv")
cat = 'sex'
cat = 'ethnicity'
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
   cat: np.unique(df['cat'])}
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


# sex
if cat == 'sex':
    perc = True
    cattotal = True
    catorder = ['Male', 'Female']
    emptypecats = True
    wsname = "3.5 - Gender (000's)"
    startrow = 9
    finishrow = 17
    finishcol = 16

# ethnicity
if cat == 'ethnicity':
    perc = True
    cattotal = True
    catorder = ['White', 'BAME']
    emptypecats = False
    wsname = "3.6 - Ethnicity (000's)"
    startrow = 8
    finishrow = 16
    finishcol = 6

if emptypecats == False:
    aggfinal = aggfinal[aggfinal['emptype'] == 'total']

"""
if (catvar == "dcms_ageband") aggfinal <- aggfinal[aggfinal$cat != "0-15 years", ]
if (catvar == "ethnicity") aggfinal <- aggfinal[aggfinal$emptype == "total" & aggfinal$cat != 0, ]
if (catvar == "qualification") aggfinal <- aggfinal[aggfinal$emptype == "total", ]
if (catvar == "ftpt") aggfinal <- aggfinal[aggfinal$emptype == "total", ]
"""

# summary table
if 'final' in globals():
    del final

for emptype in aggfinal['emptype'].unique():
    
    aggfinaltemp = aggfinal[aggfinal['emptype'] == emptype]
    aggfinaltemp.rename(columns={'count': emptype}, inplace=True)
    emptable = aggfinaltemp[['sector', emptype, cat]].set_index([cat, 'sector']).sort_index().unstack(cat)
    #emptable.columns = emptable.columns.droplevel(0)
    total = emptable.sum(axis=1)
    total.name = 'Total'

    emptable = emptable.reindex(level=1, columns=catorder)
        
    empnames = []
    if perc:
        for mycol in emptable.columns.levels[1]:
            emptable.loc[:, (emptype, mycol + '_perc')] = emptable[emptype][mycol] / total * 100
            empnames.append(mycol)
            empnames.append(mycol + '_perc')
        emptable = emptable.reindex(level=1, columns=empnames)
    
    # emptype total
    if cattotal:
        emptable.loc[:, (emptype, 'Total')] = total
        #emptable = pd.concat([emptable, total], axis=1)
    
    if 'final' in globals():
        final = pd.concat([final, emptable], axis=1)
    else:
        final = emptable.copy()

# replace NaNs with 0
final = final.fillna(0)
        
# reorder rows
myroworder = ["civil_society", "creative", "culture", "digital", "gambling", "sport", "telecoms", "all_dcms", "total_uk"]
final = final.reindex(myroworder)

data = final.copy()

# generate data
final.columns
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

data

# make sure there is not only 1 zero in non total cat levels
for row in data.index:
    for emptype in cat1:
        for sex in cat2[:-1]:
            if sum(data.loc[row, emptype].isin([0])) == 1:
                if data.loc[row, (emptype, cat2[0])] == 0:
                    data.loc[row, (emptype, cat2[1])] = 0
                else:
                    data.loc[row, (emptype, cat2[0])] = 0


data.dtypes



# store anonymised values as 0s for comparison and data types

# check data against publication --------------------------------------------

# read in publication data
from openpyxl import load_workbook, Workbook
from openpyxl.utils.dataframe import dataframe_to_rows

wb = load_workbook('DCMS_Sectors_Economic_Estimates_Employment_2016_tables.xlsx')
ws = wb[wsname]


exceldata = ws.values
exceldata = list(exceldata)
newdata = []
for row in range(startrow - 1, finishrow):
    listrow = list(exceldata[row][1:finishcol])
    listrow = [0 if x == '-' else x for x in listrow]
    newdata.append(listrow)

exceldataframe = pd.DataFrame(newdata, index=data.index, columns=data.columns)
exceldataframe.dtypes

# compare computed and publication data
difference = data - exceldataframe
if sum((difference > 1).any()) != 0:
    print('datasets dont match')
    
def test_datamatches():
    assert sum((difference > 1).any()) == 0



# write data to excel template
wb = load_workbook('DCMS_Sectors_Economic_Estimates_Employment_2016_tables_Template_unmerged.xlsx')

"""
wb.sheetnames
ws = wb['Contents']
mycell = ws['B8'].value
ws.cell(row=1, column=1, value=10) #assigns value of 10 to cell
ws['A1'].value
"""

ws = wb[wsname]
rows = dataframe_to_rows(data, index=False, header=False)

for r_idx, row in enumerate(rows, 1):
    for c_idx, value in enumerate(row, 1):
         ws.cell(row=r_idx + startrow - 1, column=c_idx + 1, value=value)

wb.save('dcms-testing2.xlsx')

# get final table with hierarchical indexes which I can check against those read in from excel (including order of rows etc), but then just output the values to the formatted excel templates

# for anonymisation, it seems something quite simple will work initially. Only gender, age, and nnsec seem like they will require rules

# for region the total won't = the sum anyway, so don't need to do annonymisation

# use openpyxl initially and move on to xlwings if necessary. xlsxwriter cannot read workbooks but should be considered if producing workbooks from scratch.













































