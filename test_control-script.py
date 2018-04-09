import pandas as pd
import numpy as np
import itertools
import pytest
import ipdb

"""
NOTES
dcms..._2016_tables_2.xlsx is the workbook we are checking against and adds some fixes: 
    adds some missing anonymisations
    rounds qualification and ftpt (rounded .5 down in some cases) numbers to 0dp

however, this approach does not make sense to add in over-anonymisation, instead temporarily remove these cases from my data. make a list below:
    sex
        employed
            total
                telecoms
                

we are currently annonymising overlap row before it is used for all_dcms which needs fixing

want to keep all cat specific adjustments in this main script, not in functions
"""




df = pd.read_csv("~/data/cleaned_2016_df.csv")


# finish cleaning data post R cleaning ========================================

df['qualification'] = df['qualification'].astype(str)
df['ftpt'] = df['ftpt'].astype(str)
df['nssec'] = df['nssec'].astype(str)

regionlookupdata = pd.read_csv('~/projects/employment/region-lookup.csv')
regionlookdict = {}
for index, row in regionlookupdata.iterrows():
    regionlookdict.update({row[0] : row[1]})

df['regionmain'] = df.GORWKR.map(regionlookdict)
df['regionsecond'] = df.GORWK2R.map(regionlookdict)



sic_mappings = pd.read_csv("~/projects/employment/sic_mappings.csv")
sic_mappings = sic_mappings[sic_mappings.sic != 62.011]
sic_mappings.sic = round(sic_mappings.sic * 100, 0)
sic_mappings.sic = sic_mappings.sic.astype(int)

from openpyxl import load_workbook, Workbook
from openpyxl.utils.dataframe import dataframe_to_rows


# BATCH TOGETHER DATA FUNCTIONS ===============================================

def make_cat_data(cat):
    
    # CLEANING DATA - adding up main and second jobs, calculating some totals, columns for sector, cat, region, count
    # there doesn't appear to be any tables which use both region and a demographic category, so simply remove region or replace cat column with it.
    import clean_data_func
    aggfinal = clean_data_func.clean_data(cat, dfcopy, sic_mappings, regionlookupdata, region)
    
    #import ipdb; ipdb.set_trace()
    
    #if mycat != 'region':
    #    aggfinal = aggfinal.groupby(['sector', cat, 'emptype'])['count'].sum().reset_index()
        #pdb.set_trace()
        
    if emptypecats == False:
        aggfinal = aggfinal[aggfinal['emptype'] == 'total']
                
    
    """
    if (catvar == "dcms_ageband") aggfinal <- aggfinal[aggfinal$cat != "0-15 years", ]
    if (catvar == "ethnicity") aggfinal <- aggfinal[aggfinal$emptype == "total" & aggfinal$cat != 0, ]
    if (catvar == "qualification") aggfinal <- aggfinal[aggfinal$emptype == "total", ]
    if (catvar == "ftpt") aggfinal <- aggfinal[aggfinal$emptype == "total", ]
    """
    
    # SUMMARISING DATA
    if cat == 'region':
        import region_summary_table_func
        final = region_summary_table_func.region_summary_table(aggfinal, cat, perc, cattotal, catorder, region)
    else:
        import summary_table_func
        final = summary_table_func.summary_table(aggfinal, cat, perc, cattotal, catorder, region)
    
    
    #pdb.set_trace()
    # ANONYMISING DATA
    import anonymise_func
    data = anonymise_func.anonymise(final, emptypecats, anoncats, cat)
    
    # add extra anonymisation to match publication
    if cat == 'sex':
        data.loc['telecoms', ('employed', 'Total')] = 0
        data.loc['telecoms', ('self employed', 'Total')] = 0
    
    # CHECK DATA MATCHES PUBLICATION
    # store anonymised values as 0s for comparison and data types
    import check_data_func
    from openpyxl import load_workbook, Workbook
    from openpyxl.utils.dataframe import dataframe_to_rows
    exceldataframe = check_data_func.check_data(data, wsname, startrow, finishrow, finishcol)
    # compare computed and publication data
    
    difference = data - exceldataframe
    
    if sum((difference > 1).any()) != 0:
        print(cat + ': datasets dont match')
    
    #pdb.set_trace()
    return [difference, data]


# write data to excel template ================================================
    
wb = load_workbook('DCMS_Sectors_Economic_Estimates_Employment_2016_tables_Template_unmerged.xlsx')

"""
#catvar <- "sex"; catorder <- c("Male", "Female"); sheet <- 14; xy <- c(2,9); perc <- TRUE; cattotal <- TRUE
catvar <- "ethnicity"; catorder <- c("White", "BAME"); sheet <- 15; xy <- c(2,8); perc <- TRUE; cattotal <- TRUE
#catvar <- "dcms_ageband"; catorder <- NA; sheet <- 16; xy <- c(2,8); perc <- FALSE; cattotal <- TRUE
#catvar <- "qualification"; catorder <- NA; sheet <- 17; xy <- c(2,7); perc <- FALSE; catorder <- c("Degree or equivalent",	"Higher Education",	"A Level or equivalent", "GCSE A* - C or equivalent",	"Other",	"No Qualification"); cattotal <- TRUE
#catvar <- "ftpt"; catorder <- c("Full time", "Part time"); sheet <- 18; xy <- c(2,8); perc <- TRUE; cattotal <- TRUE
#catvar <- "nssec"; catorder <- c("More Advantaged Group (NS-SEC 1-4)", "Less Advantaged Group (NS-SEC 5-8)"); sheet <- 19; xy <- c(2,8); perc <- FALSE; cattotal <- FALSE


if (catvar == "qualification") df <- df[df[, catvar] != "dont know" & !is.na(df[, catvar]), ]
if (catvar == "ftpt") df <- df[df[, catvar] %in% catorder, ]
if (catvar == "nssec") df <- df[df[, catvar] %in% catorder, ]
"""

differencelist = {}
for mycat in ['sex', 'ethnicity', 'dcms_ageband', 'qualification', 'ftpt', 'nssec', 'region']:
    
    anoncats = []
    
    # sex
    if mycat == 'sex':
        perc = True
        cattotal = True
        catorder = ['Male', 'Female']
        emptypecats = True
        wsname = "3.5 - Gender (000's)"
        startrow = 9
        finishrow = 17
        finishcol = 16
        region = False
    
    # ethnicity
    if mycat == 'ethnicity':
        perc = True
        cattotal = True
        catorder = ['White', 'BAME']
        emptypecats = False
        wsname = "3.6 - Ethnicity (000's)"
        startrow = 8
        finishrow = 16
        finishcol = 6
        region = False
        
        # ethnicity
    if mycat == 'dcms_ageband':
        perc = False
        cattotal = True
        catorder = ['16-24 years', '25-39 years', '40-59 years', '60 years +']
        emptypecats = True
        wsname = "3.7 - Age (000's)"
        startrow = 8
        finishrow = 16
        finishcol = 16
        region = False
        
    if mycat == 'qualification':
        perc = False
        cattotal = True
        catorder = ["Degree or equivalent",	"Higher Education",	"A Level or equivalent", "GCSE A* - C or equivalent",	"Other",	"No Qualification"]
        emptypecats = False
        wsname = "3.8 - Qualification (000's)"
        startrow = 7
        finishrow = 15
        finishcol = 8
        anoncats = ['Other', 'No Qualification']
        region = False
                
    if mycat == 'ftpt':
        perc = True
        cattotal = True
        catorder = ['Full time', 'Part time']
        emptypecats = False
        wsname = "3.9 - Fulltime Parttime (000's)"
        startrow = 8
        finishrow = 16
        finishcol = 6
        region = False
        
    if mycat == 'nssec':
        perc = False
        cattotal = False
        catorder = ["More Advantaged Group (NS-SEC 1-4)", "Less Advantaged Group (NS-SEC 5-8)"]
        emptypecats = True
        wsname = "3.10 - NS-SEC (000's)"
        startrow = 8
        finishrow = 16
        finishcol = 7
        region = False
        
    if mycat == 'region':
        perc = True
        cattotal = False
        catorder = ["More Advantaged Group (NS-SEC 1-4)", "Less Advantaged Group (NS-SEC 5-8)"]
        emptypecats = True
        wsname = "3.3 - Region (000's)"
        startrow = 8
        finishrow = 21
        finishcol = 6
        region = True
        #import ipdb; ipdb.set_trace()
        
    dfcopy = df.copy()
    if mycat == 'qualification':
        dfcopy = dfcopy[dfcopy.qualification != 'dont know']
        dfcopy = dfcopy[dfcopy.qualification != 'nan']
    
    
    mylist = make_cat_data(mycat)
    data = mylist[1]
    difference = mylist[0]    
    differencelist.update({mycat : difference})
    
    ws = wb[wsname]
    rows = dataframe_to_rows(data, index=False, header=False)
    
    for r_idx, row in enumerate(rows, 1):
        for c_idx, value in enumerate(row, 1):
             ws.cell(row=r_idx + startrow - 1, column=c_idx + 1, value=value)
 
  
"""
wsname = "3.5 - Gender (000's)"
startrow = 9
finishrow = 17
finishcol = 16
"""

# marks=pytest.mark.xfail
import pytest
@pytest.mark.parametrize('test_input,expected', [
    pytest.param('sex', 0, marks=pytest.mark.basic),
    pytest.param('ethnicity', 0, marks=pytest.mark.basic),
    pytest.param('dcms_ageband', 0, marks=pytest.mark.basic),
    pytest.param('qualification', 0, marks=pytest.mark.xfail), # publication numbers dont add up - go through with penny - turn's out there is an extra column which is hidden by the publication called don't know which explains all this
    pytest.param('ftpt', 0, marks=pytest.mark.basic),
    pytest.param('nssec', 0, marks=pytest.mark.basic),
])
def test_datamatches(test_input, expected):
    assert sum((differencelist[test_input] < -0.05).any()) == expected
    assert sum((differencelist[test_input] > 0.05).any()) == expected


wb.save('dcms-testing2.xlsx')

# get final table with hierarchical indexes which I can check against those read in from excel (including order of rows etc), but then just output the values to the formatted excel templates

# for anonymisation, it seems something quite simple will work initially. Only gender, age, and nnsec seem like they will require rules

# for region the total won't = the sum anyway, so don't need to do annonymisation

# use openpyxl initially and move on to xlwings if necessary. xlsxwriter cannot read workbooks but should be considered if producing workbooks from scratch.













































