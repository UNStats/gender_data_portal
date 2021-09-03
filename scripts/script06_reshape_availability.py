
import utils
import utils2
import json
import copy

# Read minset_indicators_catalog
minset_indicators_catalog = utils.xlsx2dict('master_data/catalog_series_old_and_new.xlsx', 0)
#print(minset_indicators_catalog[0])

# Read availability
x = utils.tsv2dictlist('availability_data/availability_bySeriesCountry.csv')
print(x[0])

# Convert lists of years into ordered lists of integers:
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

    if record['MINSET_SERIES'] == 'Sc5211ad2' and record['REF_AREA'] == '604':
        print(record['years'])
        print(record['disaggregated_by_age'])
        print(record['disaggregated_by_sex'])

#----------



availability_bySeriesCountry_long = []

for idx, record in enumerate(x):

    # if idx != 0:
    #     continue

    for s in minset_indicators_catalog:
        if record['MINSET_SERIES'] == s['MINSET_SERIES']:
            domain_id = s['DOMAIN_ID']
            domain_name = s['DOMAIN_NAME']

    for y in record['years']:

        d = copy.deepcopy(record)

        d['domain_id'] = domain_id
        d['domain_name'] = domain_name

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

    for s in minset_indicators_catalog:
        if record['MINSET_SERIES'] == s['MINSET_SERIES']:
            domain_id = s['DOMAIN_ID']
            domain_name = s['DOMAIN_NAME']
    
    
    d['domain_id'] = domain_id
    d['domain_name'] = domain_name

    for c in common_cols:
        d[c] = record[c]

    # Make sure that these are copies - otherwise they will point to the same object
    d_sex = copy.deepcopy(d)
    d_age = copy.deepcopy(d)

    #------------------------

    
    if record['MINSET_SERIES'] == 'Sc5211ad2' and record['REF_AREA'] == '604':
        print(record['years'])
        print(range(1990,2021))

    d['years_n'] = len(record['years'])
    
    for y in range(1990, 2021):

        if y in record['years']:
            d['y_'+str(y)] = 1
            if record['MINSET_SERIES'] == 'Sc5211ad2' and record['REF_AREA'] == '604':
                print(y)
                print(record['years'])
                print('yes')
        else:
            d['y_'+str(y)] = 0
            if record['MINSET_SERIES'] == 'Sc5211ad2' and record['REF_AREA'] == '604':
                print(y)
                print(record['years'])
                print('no')

    availability_bySeriesCountry_wide.append(d)

    # if record['MINSET_SERIES'] == 'Sc5211ad2' and record['REF_AREA'] == '604':
    #     print(d)

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

    # if record['MINSET_SERIES'] == 'Sc5211ad2' and record['REF_AREA'] == '604':
    #     print(d)

newlist = sorted(availability_bySeriesCountry_wide, key=lambda k: k['sort_order']) 
newlist_sex = sorted(disaggregatedSex_bySeriesCountry_wide, key=lambda k: k['sort_order']) 
newlist_age = sorted(disaggregatedAge_bySeriesCountry_wide, key=lambda k: k['sort_order']) 

utils.dictList2csv(newlist, 'availability_data/availability_bySeriesCountry_wide.csv')
utils.dictList2csv(newlist_sex, 'availability_data/availability_bySeriesCountry_wide_disaggregatedSex.csv')
utils.dictList2csv(newlist_age, 'availability_data/availability_bySeriesCountry_wide_disaggregatedAge.csv')
