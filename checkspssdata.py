#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 30 17:44:07 2018

@author: max.unsted
"""

        # in the final comparison for the region table, there is a discrepancy for west midlands self employed. to understand where this comes from, I will compare spss output for the region table with the equivalent from my program. this is the output from clean_data() for region, filtered to only include sector = 'total uk' (the totaluk part of the clean_data() data stack is completely unadulterated expect for filtering by emptype flag e.g. INECAC05 = 1) and then restricted to spss sics and aggregated by region and sic. however, with this comparison, there is no discrepancy for west midlands, only mainemp and secondsemp outside UK.
        # aggcheck has too few outside UK for some reason. spssdf is as is (except for changing the grammer of some region labels), so obviously the definition of outisde UK for aggcheck is missing some cases. In order to view the records causing the discrepancy between each Outside UK numbers, we need to compare prior to aggregation, so I have output the first of the four subsets (mainemp) from spss and filtered to include only Outside UK and cases with a mainemp flag and inecac = employee - this gives 28 records with a total weighted count of 9259. next, aggregate this subset by sic to check it matches the standard output from spss.
        
        # so, I have mainemp from spss, which has no sic filtering, the sic filtering is applied in excel - what?? this isnt right. but the point is that my data does not match the spss output for outside uk, prior to sic filtering. REMEMBER running spss gives the wrong results on my mac - some problem with the unicode formatting - it errors with unicode off, and gives wrong result with unicode on.
        def check_spss_out(agg, spss_xls, excelcatname):
            aggcheck = agg.fillna(0)
            # reduce down to desired aggregate
            
            # need to drop the sic/cat combinations that are duplicated for difference sectors
            aggcheck = aggcheck[aggcheck['sector'] == 'total_uk']
            aggcheck = aggcheck.drop('sector', axis=1)
            aggcheck = aggcheck[aggcheck['sic'].isin(mylist)]
            aggcheck = aggcheck.groupby(['sic', cat]).sum()
            aggcheck = aggcheck.reset_index(['sic', cat])
            #aggcheck = aggcheck[aggcheck['sic'] != -1]
            if cat == 'region':
                aggcheck = aggcheck[aggcheck[cat] != 'missing region']
    
            
            regionlookupdata2 = pd.read_csv('~/projects/employment/region-label-update.csv')
            regionlookdict2 = {}
            for index, row in regionlookupdata2.iterrows():
                regionlookdict2.update({row[0] : row[1]})
    
            # load workbook
            spsswb = pd.ExcelFile(spss_xls + '.xls')
            # print(spsswb.sheet_names)
            spssdf = spsswb.parse(spss_xls)
            
            # change region label grammer to match aggcheck
            if cat == 'region':
                spssdf['Region'] = spssdf.Region.map(regionlookdict2)
            # remove decimal from SIC
            spssdf['sic'] = spssdf['SIC'].str[0:2] + spssdf['SIC'].str[3:5]
            spssdf.sic = spssdf.sic.astype('float64')
            # match column name and order to aggcheck
            spssdf = spssdf[['sic', excelcatname, 'M_E_DCMS', 'S_E_DCMS', 'M_SE_DCMS', 'S_SE_DCMS']]
            spssdf.columns = aggcheck.columns
    
            wmcheck = spssdf[spssdf['sic'].isin(np.unique(sic_mappings.sic))]
            wmcheck['semp'] = wmcheck['mainselfemp'] + wmcheck['secondselfemp']
            
            
            aggcheck = aggcheck.set_index(['sic', cat])
            spssdf = spssdf.set_index(['sic', cat])
            #aggcheck.to_csv('~/projects/employment/aggcheck_forpenny.csv')
            
            
            return {'aggcheck': aggcheck, 'spssdf': spssdf}
        
        if cat == 'region':
            aggcheck = check_spss_out(agg, '2016_regions_4digit', 'Region')['aggcheck']
            spssdf = check_spss_out(agg, '2016_regions_4digit', 'Region')['spssdf']
        #both = check_spss_out(agg, '2016_sex_4digit', 'SEX')    
        
            
        """    
        spsswm = spssdf.xs('North East',level=1,axis=0)
        acwm = aggcheck.xs('North East',level=1,axis=0)
        wmdiff = spsswm - acwm
        """
