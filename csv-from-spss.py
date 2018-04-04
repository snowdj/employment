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
