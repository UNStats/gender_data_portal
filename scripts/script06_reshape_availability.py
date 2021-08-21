
import utils
import utils2
import json
import copy

# 1. Read availability
x = utils.tsv2dictlist('availability_data/availability_bySeriesCountry.csv')
print(x[0])

# convert years to ordered list of integers:

for idx, record in enumerate(x):

    years = record['years'].replace('[','').replace(']','').split(', ')
    years.sort()
    record['years'] = [int(float(y)) for y in years]

    disaggregated_by_age =  record['disaggregated_by_age'].replace('[','').replace(']','').split(', ')
    disaggregated_by_age.sort()
    record['disaggregated_by_age'] = [int(float(y)) for y in disaggregated_by_age if y]

    disaggregated_by_sex =  record['disaggregated_by_sex'].replace('[','').replace(']','').split(', ')
    disaggregated_by_sex.sort()
    record['disaggregated_by_sex'] = [int(float(y)) for y in disaggregated_by_sex if y]

    # print(record['years'])
    # print(record['disaggregated_by_age'])
    # print(record['disaggregated_by_sex'])

availability_bySeriesCountry_long = []

for idx, record in enumerate(x):

    # if idx != 0:
    #     continue

    for y in record['years']:

        d = copy.deepcopy(record)

        d['years'] = y
        d['years_n'] = len(record['years'])

        # print(f"{y=}")

        # print(f"{record['disaggregated_by_age']=}")
        
        if record['disaggregated_by_age']:
            if y in record['disaggregated_by_age']:
                d['disaggregated_by_age'] = True
            else:
                d['disaggregated_by_age'] = False
        else:
            d['disaggregated_by_age'] = False
        
        if record['disaggregated_by_sex']:
            if y in record['disaggregated_by_sex']:
                d['disaggregated_by_sex'] = True
            else:
                d['disaggregated_by_sex'] = False
        else:
            d['disaggregated_by_sex'] = False

        availability_bySeriesCountry_long.append(d)

newlist = sorted(availability_bySeriesCountry_long, key=lambda k: k['sort_order']) 

utils.dictList2csv(newlist, 'availability_data/availability_bySeriesCountry_long.csv')


availability_bySeriesCountry_wide = []
disaggregatedSex_bySeriesCountry_wide = []
disaggregatedAge_bySeriesCountry_wide = []

common_cols = ['INDICATOR_ID',
    'INDICATOR_DESC',
    'MINSET_SERIES',
    'MINSET_SERIES_DESC',
    'REF_AREA',
    'REF_AREA_DESC',
    'ISO3',
    'X',
    'Y',
    'SDG_REGION',
    'UNmember',
    'GEOLEVEL_Desc',
    'GEOLEVEL',
    'Select195',
    'sort_order']

for idx, record in enumerate(x):

    d = dict()

    for c in common_cols:
        d[c] = record[c]

    d_sex = d
    d_age = d

    #------------------------

    d['years_n'] = len(record['years'])
    
    for y in range(1990, 2021):
        if y in record['years']:
            d['y_'+str(y)] = 1
        else:
            d['y_'+str(y)] = 0

    availability_bySeriesCountry_wide.append(d)

    #------------------------

    d_sex['years_n'] = len(record['disaggregated_by_sex'])
    
    for y in range(1990, 2021):
        if y in record['disaggregated_by_sex']:
            d_sex['y_'+str(y)] = 1
        else:
            d_sex['y_'+str(y)] = 0

    disaggregatedSex_bySeriesCountry_wide.append(d_sex)

    #------------------------

    d_age['years_n'] = len(record['disaggregated_by_age'])
    
    for y in range(1990, 2021):
        if y in record['disaggregated_by_age']:
            d_age['y_'+str(y)] = 1
        else:
            d_age['y_'+str(y)] = 0

    disaggregatedAge_bySeriesCountry_wide.append(d_age)

newlist = sorted(availability_bySeriesCountry_wide, key=lambda k: k['sort_order']) 
newlist_sex = sorted(disaggregatedSex_bySeriesCountry_wide, key=lambda k: k['sort_order']) 
newlist_age = sorted(disaggregatedAge_bySeriesCountry_wide, key=lambda k: k['sort_order']) 

utils.dictList2csv(newlist, 'availability_data/availability_bySeriesCountry_wide.csv')
utils.dictList2csv(newlist_sex, 'availability_data/availability_bySeriesCountry_wide_disaggregatedSex.csv')
utils.dictList2csv(newlist_age, 'availability_data/availability_bySeriesCountry_wide_disaggregatedAge.csv')
