import utils
import json
from os import listdir
from os.path import isfile, join

# Read csv file

datafiles = [f for f in listdir('series_data/') if isfile(join('series_data/', f))]

all_columns_list = set()

# for df in datafiles:

#     if df !='ind_1__series_72.csv':
#         continue

#     f = utils.tsv2dictlist('series_data/'+df)
#     all_columns = set(f[0].keys())

#     #---------------------------------
#     # Check that X,Y and SORUCE_DETAIL_URL are all uppercase
#     #---------------------------------
#     if ('x' in all_columns):
#         print (f"x Exists in {df}")

#     if ('y' in all_columns):
#         print (f"y Exists in {df}")

#     if ('SOURCE_DETAIL_url' in all_columns):
#         print (f"SOURCE_DETAIL_url Exists in {df}")

#     all_columns_list.update(all_columns)

# all_columns_list = list(all_columns_list)

# all_columns_list.sort()

# print(f'\nList of all columns across all files:\n{all_columns_list}')


# #----------- Iterate here ---------#

availability = []

for f in datafiles:
    # if f !='ind_1__series_72.csv':
    #     continue

    print(f"\nCurrent file: {f}")

    f2 = utils.tsv2dictlist('series_data/' + f)
    
    # Get the list of "columns" in current file
    all_columns = f2[0].keys()
    
    # Get the list of "dimension columns" (those thare are used to identify time series)

    non_TSK_columns = ['TIME_PERIOD',
                    'COMMENT_OBS',
                    'INDICATOR_NUM',
                    'ISO3',
                    'LOWER_BOUND',
                    'LOWER_BOUND_MODIFIER',
                    'NATURE',
                    'NATURE_DESC',
                    'OBS_VALUE',
                    'OBS_VALUE_MODIFIER',
                    'SDG_REGION',
                    'SOURCE_DETAIL',
                    'SOURCE_DETAIL_URL',
                    'SOURCE_YEAR',
                    'TIME_DETAIL',
                    'UNIT_MEASURE',
                    'UNIT_MEASURE_DESC',
                    'UPPER_BOUND',
                    'UPPER_BOUND_MODIFIER',
                    'VALUE_CATEGORY',
                    'VALUE_CATEGORY_DESC',
                    'REPORTING_TYPE', 
                    'REPORTING_TYPE_DESC',
                    'X',
                    'Y']

    TSK_columns = [x for x in all_columns if x not in non_TSK_columns]
    
    unique_TSK_values = utils.unique_dicts(utils.subdict_list(f2, non_TSK_columns, exclude=True))

    # print(unique_TSK_values[0])

    utils.dictList2tsv(unique_TSK_values, 'test.txt')

    #------------------------
    # Test for unique records
    #------------------------


    # has_duplicates = []

    # for ts in unique_TSK_values:

    #     # Number of records in time series group:
    #     x = utils.select_dict(f2, ts)
    #     N = len(x)

    #     # Number of unique years in time series group:
    #     unique_years = utils.unique_dicts(utils.subdict_list(x, ['TIME_PERIOD']))
    #     n = len(unique_years)

    #     if (N != n):
    #         has_duplicates.append(ts)

    # if(len(has_duplicates)>0):
    #     print(f"file {f} has_duplicates")

    #---------------------------------------
    # Availability calculations
    #---------------------------------------

    

    for ts in unique_TSK_values:

        ts_availability = dict(ts)  # or orig.copy()

        # Get list of years (as integers):

        ts_years = utils.subdict_list(utils.select_dict(f2, ts, keep=True), ['TIME_PERIOD'])   
        years = []
        for y in ts_years:
            years.append(int(float(y['TIME_PERIOD'])))
        # print(years)
        years.sort()
    # Get availability stats: min(years), max (years), Number of years
        ts_availability['years'] = years
        ts_availability['min_year'] = min(years)
        ts_availability['max_year'] = max(years)
        ts_availability['N_years'] = len(years)

        availability.append(ts_availability)

utils.dictList2tsv(availability, 'availability_data/ts_series_availability.txt')


## Aggregate availability

# 1. How many countries have data (at least 1 data point/year) for each series.

 series = utils.subdict_list(availability, 'MINSET_SERIES')

 for s in series:
     n_countries = len(unique.select_dict(availability, {'MINSET_SERIES': s['MINSET_SERIES']}, keep=True))
 
 
 utils.subdict_list(availability, ['INDICATOR_ID','INDICATOR_DESC','MINSET_SERIES','MINSET_SERIES_DESC'])


# 2. How many countries have at least 1 data point since 2015? At least 2 data points since 2015?





