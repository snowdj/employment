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
    sic_level = False
    
    import clean_data_func
    agg = clean_data_func.clean_data(cat, dfcopy, sic_mappings, regionlookupdata, region, sic_level)
    
    spsslist = """
    1820, 2611, 2612, 2620, 2630, 2640, 2680, 3012, 3212, 3220, 3230, 4651, 4652, 4763, 4764, 4910, 4932, 4939, 5010, 5030, 5110, 5510, 5520, 5530, 5590, 5610, 5621, 5629, 5630, 5811, 5812, 5813, 5814, 
    5819, 5821, 5829, 5911, 5912, 5913, 5914, 5920, 6010, 6020, 6110, 6120, 6130, 6190, 6201, 6202, 6203, 6209, 6311, 6312, 6391, 6399, 6820, 7021, 7111, 7311, 7312, 7410, 7420, 7430, 7711, 7721, 
    7722, 7729, 7734, 7735, 7740, 7911, 7912, 7990, 8230, 8551, 8552, 9001, 9002, 9003, 9004, 9101, 9102, 9103, 9104, 9200, 9311, 9312, 9313, 9319, 9321, 9329, 9511, 9512 """
    spsslist = spsslist.replace('\n', '')
    spsslist = spsslist.replace('\t', '')
    spsslist = spsslist.replace(' ', '')
    mylist = np.array(spsslist.split(","))
    
    if cat == 'region':
        aggcheck = agg.fillna(0)
        # reduce down to desired aggregate
        aggcheck = aggcheck.drop('sector', axis=1)
        aggcheck = aggcheck.groupby(['sic', cat]).sum()
        aggcheck = aggcheck.reset_index(['sic', cat])
        #aggcheck = aggcheck[aggcheck['sic'] != -1]
        aggcheck = aggcheck[aggcheck['region'] != 'missing region']
        aggcheck = aggcheck[aggcheck['sic'].isin(mylist)]
        
        
        # load workbook
        spsswb = pd.ExcelFile('2016_regions_4digit.xls')
        # print(spsswb.sheet_names)
        spssdf = spsswb.parse('2016_regions_4digit')
        spssdf['sic'] = spssdf['SIC'].str[0:2] + spssdf['SIC'].str[3:5]
        spssdf.sic = spssdf.sic.astype('float64')
        spssdf = spssdf[['sic', 'Region', 'M_E_DCMS', 'M_SE_DCMS', 'S_E_DCMS', 'S_SE_DCMS']]
        spssdf.columns = aggcheck.columns
        
        aggcheck = aggcheck.set_index(['sic', 'region'])
        spssdf = spssdf.set_index(['sic', 'region'])
        
        spsswm = spssdf.xs('North West',level=1,axis=0)
        acwm = aggcheck.xs('North West',level=1,axis=0)
        wmdiff = spsswm - acwm
    
    import aggregate_data_func
    aggfinal = aggregate_data_func.aggregate_data(cat, agg, sic_mappings, regionlookupdata, region, sic_level)
    
    #import clean_data_func_new
    #aggfinal_new = clean_data_func_new.clean_data_new(cat, dfcopy, sic_mappings, regionlookupdata, region)
    
    #aggfinal = aggfinal[['sector', 'sex', 'emptype', 'count']].sort_values(by=['sector', 'sex', 'emptype']).set_index(['sector', 'sex', 'emptype'])
    #aggfinal_new = aggfinal_new[['sector', 'sex', 'emptype', 'count']].sort_values(by=['sector', 'sex', 'emptype']).set_index(['sector', 'sex', 'emptype'])
    #difference = aggfinal_new - aggfinal
    
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
    pytest.param('region', 0, marks=pytest.mark.basic),
])
def test_datamatches(test_input, expected):
    assert sum((differencelist[test_input] < -0.05).any()) == expected
    assert sum((differencelist[test_input] > 0.05).any()) == expected


wb.save('dcms-testing2.xlsx')

# get final table with hierarchical indexes which I can check against those read in from excel (including order of rows etc), but then just output the values to the formatted excel templates

# for anonymisation, it seems something quite simple will work initially. Only gender, age, and nnsec seem like they will require rules

# for region the total won't = the sum anyway, so don't need to do annonymisation

# use openpyxl initially and move on to xlwings if necessary. xlsxwriter cannot read workbooks but should be considered if producing workbooks from scratch.













































