import utils 
import json
from os import listdir
from os.path import isfile, join

#---------------
# Set availability parameters
#---------------

# What is the latest year that has passed
current_year = 2020

# Read csv file

geo = utils.xlsx2dict('master_data/CL_AREA.xlsx', 0)

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

availability_by_country = []

for f in datafiles:
    
    availability = []

    if f not in ['ind_1__series_72.csv', 'ind_1__series_87.csv']:
        continue

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

        ref_area = ts['REF_AREA']
        #print(ref_area)

        geo_info = utils.select_dict(geo, {'REF_AREA': ref_area}, keep=True)
        #print(geo_info[0])

        ts_availability['UNmember'] = geo_info[0]['UNmember']
        ts_availability['GEOLEVEL'] = geo_info[0]['GEOLEVEL']
        ts_availability['GEOLEVEL_Desc'] = geo_info[0]['GEOLEVEL_Desc']
        ts_availability['SDG_REGION'] = geo_info[0]['SDG_REGION']

        # Get list of years (as integers):

        ts_years = utils.subdict_list(utils.select_dict(f2, ts, keep=True), ['TIME_PERIOD'])  
        
         
        years = []
        for y in ts_years:
            y['TIME_PERIOD']=int(float(y['TIME_PERIOD']))
            years.append(y['TIME_PERIOD'])

        #print(years)
        years.sort()
    # Get availability stats: min(years), max (years), Number of years, Number of years after 2015
        ts_availability['years'] = years
        ts_availability['min_year'] = min(years)
        ts_availability['max_year'] = max(years)
        ts_availability['N_years'] = len(years)
        ts_availability['N_years_lag2'] = len([y for y in years if y >= current_year - 2])
        ts_availability['N_years_lag5'] = len([y for y in years if y >= current_year - 5])
        ts_availability['N_years_lag10'] = len([y for y in years if y >= current_year - 10])


        availability.append(ts_availability)

    
    utils.dictList2tsv(availability, 'availability_data/ts_availability'+f+'.txt')

    # Select TS for each country

    countries =  utils.unique_dicts(utils.subdict_list(availability, ['REF_AREA']) )

    countries =  [ i['REF_AREA'] for i in countries]


    for c in countries:
        # Select TS for country c:

        availability_country = dict()

        # if c not in ['8','32']:
        #     continue

        print(f"passed {c} in ['8','32']")

        data = utils.select_dict(availability, {'REF_AREA': c})

        availability_country['INDICATOR_ID'] = data[0]['INDICATOR_ID']
        availability_country['INDICATOR_DESC'] = data[0]['INDICATOR_DESC']
        availability_country['MINSET_SERIES'] = data[0]['MINSET_SERIES']
        availability_country['MINSET_SERIES_DESC'] = data[0]['MINSET_SERIES_DESC']
        availability_country['REF_AREA'] = data[0]['REF_AREA']
        availability_country['REF_AREA_DESC'] = data[0]['REF_AREA_DESC']

        availability_country['max_year'] = max([ ts['max_year'] for ts in data ])
        availability_country['min_year'] = min([ ts['min_year'] for ts in data ])
        availability_country['data_points'] = sum([ ts['N_years'] for ts in data ])
        availability_country['data_points_lag5'] = sum([ ts['N_years_lag5'] for ts in data ])

        years = []
        age_categories = []
        sex_categories = []
        for ts in data:
            years.extend(ts['years'])
            if('SEX' in ts.keys()):
                sex_categories.append(ts['SEX'])
            if('AGE' in ts.keys()):
                age_categories.append(ts['AGE'])
        
        years = list(set(years))
        age_categories = list(set(age_categories))
        sex_categories = list(set(sex_categories))

        print(years)
        print(age_categories)
        print(sex_categories)

        # Age disaggregation by year:

        d_age = dict()

        for k in age_categories:
            
            merged_years = []

            data_k = utils.select_dict(data, {'AGE': k})
        
            for i in data_k:
                merged_years.extend(i['years'])

            d_age[k] = list(set(merged_years))

        print(d_age)

        # Sex disaggregation by year:

        d_sex = dict()

        for k in sex_categories:
            
            merged_years = []

            data_k = utils.select_dict(data, {'SEX': k})
        
            for i in data_k:
                merged_years.extend(i['years'])

            d_sex[k] = list(set(merged_years))

        print(d_sex)

        # -----------------------------

        years_d_sex = []
        for y in years:
            n = 0
            for i in sex_categories:
                if y in d_sex[i]:
                    n = n + 1
            if n>1:
                years_d_sex.append(y)
        
        print(years_d_sex)

        # -----------------------------

        years_d_age = []
        for y in years:
            n = 0
            for i in age_categories:
                if y in d_age[i]:
                    n = n + 1
            if n>1:
                years_d_age.append(y)
        
        print(years_d_age)


        availability_country['disaggregated_by_age'] = years_d_age
        availability_country['disaggregated_by_age_n'] = len(years_d_age)
        availability_country['disaggregated_by_sex'] = years_d_sex
        availability_country['disaggregated_by_sex_n'] = len(years_d_sex)

        availability_by_country.append(availability_country)


utils.dictList2tsv(availability_by_country, 'availability_data/availability_by_country.txt')



            




        

  #      Years for which there is any data = list(set(years(1) + years(2) + ... ))




#=======================================================================================
# # 1. How many countries have data (at least 1 data point/year) for each series.

#  series = utils.subdict_list(availability, 'MINSET_SERIES')

#  for s in series:
#      n_countries = len(unique.select_dict(availability, {'MINSET_SERIES': s['MINSET_SERIES']}, keep=True))
 
 
#  utils.subdict_list(availability, ['INDICATOR_ID','INDICATOR_DESC','MINSET_SERIES','MINSET_SERIES_DESC'])


# # 2. How many countries have at least 1 data point since 2015? At least 2 data points since 2015?





