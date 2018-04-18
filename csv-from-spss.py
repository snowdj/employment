import pandas as pd
import pdb

mytext = """
	(1=1) (2=1) 
	(3=2) (4=2) (5=2)
	(6=3) (7=3) (8=3)
	(9=4)
	(10=5) (11=5)
	(12=6)
	(13=7) (14=7) (15=7)
	(16=8) 
	(17=9) 
	(18=10)
	(19=11) (20=11)
	(21=12)
	(22=13) (23=13)"""
mytext = mytext.replace('\n', '')
mytext = mytext.replace('\t', '')
mytext = mytext.replace(' ', '')

"""
lookup = []
for i, x in enumerate(mytext):
    rowlist = []
    if mytext[i] == '(':
        rowlist.append(mytext[i + 1])
        rowlist.append(mytext[i + 3])
        lookup.append(rowlist)
"""

lookup = []
for i, x in enumerate(mytext):
    rowlist = []
    if mytext[i] == '(':
        for j, y in enumerate(mytext[i:]):
            if y == ')':
                close = j
                break
        paren = mytext[(i + 1):(i + close)]
        equalsign = paren.find('=')
        rowlist.append(paren[:(equalsign)])
        rowlist.append(paren[(equalsign + 1):])
        lookup.append(rowlist)

data = pd.DataFrame(lookup, columns = ['original', 'mapno'])

regionnames = {1 : "North East",
	2: "North West",
	3 :"Yorkshire & Humberside",
	4: "East Midlands",
	5: "West Midlands",
	6: "East of England",
	7: "Greater London",
	8: "South East",
	9: "South West",
	10: "Wales",
	11: "Scotland",
	12: "Northern Ireland",
	13: "Outside UK"}
data.mapno = pd.to_numeric(data.mapno)
data.mapno = data.mapno.map(regionnames)
data.to_csv('~/projects/employment/region-lookup.csv', index=False)

spsslist = """
1820, 2611, 2612, 2620, 2630, 2640, 2680, 3012, 3212, 3220, 3230, 4651, 4652, 4763, 4764, 4910, 4932, 4939, 5010, 5030, 5110, 5510, 5520, 5530, 5590, 5610, 5621, 5629, 5630, 5811, 5812, 5813, 5814, 
5819, 5821, 5829, 5911, 5912, 5913, 5914, 5920, 6010, 6020, 6110, 6120, 6130, 6190, 6201, 6202, 6203, 6209, 6311, 6312, 6391, 6399, 6820, 7021, 7111, 7311, 7312, 7410, 7420, 7430, 7711, 7721, 
7722, 7729, 7734, 7735, 7740, 7911, 7912, 7990, 8230, 8551, 8552, 9001, 9002, 9003, 9004, 9101, 9102, 9103, 9104, 9200, 9311, 9312, 9313, 9319, 9321, 9329, 9511, 9512 """
spsslist = spsslist.replace('\n', '')
spsslist = spsslist.replace('\t', '')
spsslist = spsslist.replace(' ', '')
mylist = np.array(spsslist.split(","))
mylist.size # 93
len(spsslist.split(",")) # 93


mytext = """
North East
North West
Yorkshire and the Humber
East Midlands
West Midlands
East of England
London
South East
South West
Wales
Scotland
Northern Ireland
All regions4
All UK"""

mylist = mytext.split('\n')
















