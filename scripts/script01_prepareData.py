import utils
import utils2
import json
import copy
from os import listdir
from os.path import isfile, join

# PROCESSING STEPS

# 1. Read / Validate Indicators catalog

# Read minset_indicators_catalog
minset_indicators_catalog = utils.xlsx2dict('master_data/CL_SERIES.xlsx', 0)
#print(minset_indicators_catalog[0])

# Read geographic areas catalog
geo = utils.xlsx2dict('master_data/CL_AREA.xlsx',0)
#print(geo)

# List of source data files
datafiles = [f for f in listdir('source_data/') if isfile(join('source_data/', f))]

# What is the latest year that has passed
current_year = 2020

series_source = []
availability_by_series_and_country = []

for fdx, f in enumerate(datafiles):

    print(f'file: {f}')

    # if f not in ['V_5__I731e1dca_data.xlsx']:
    #      continue
    
    # if fdx > 3:
    #     continue

    # if not f.startswith('Q'):
    #     continue

    # Read source data file:
    x = utils.xlsx2dict('source_data/'+ f, 0, {'INDICATOR_ID':str, 'MINSET_SERIES':str, })
    # print(x[0]) 

    # Ensure column names are uppercase
    x = utils2.col_names_to_uppercase(x)

    #-----------------------------------
    # List of unique indicator-series-sourceYear combinations
    #-----------------------------------
    x1_list = utils.unique_dicts( utils.subdict_list(x, ['INDICATOR_LABEL','INDICATOR_ID','MINSET_SERIES', 'SOURCE_YEAR'] ) )
    #print(x1_list)

    #-----------------------------------
    # List of unique indicator-series combinations:
    #-----------------------------------
    x2_list = utils.unique_dicts( utils.subdict_list(x1_list, ['INDICATOR_LABEL','INDICATOR_ID','MINSET_SERIES'] ) )
    #print(x2_list)

    #-----------------------------------
    # Trasformations on individual series
    #-----------------------------------

    for jdx, j in enumerate(x2_list):

        #current indicator and series id's:
        i = j['INDICATOR_ID']
        s = j['MINSET_SERIES']
        i_label = j['INDICATOR_LABEL'].replace('.','_')

        for mic_i in minset_indicators_catalog:

            if mic_i['MINSET_SERIES'] == s:
                sort_order=mic_i['SORT_ORDER']
                SDG_SeriesID=mic_i['SDG_SeriesID']
                ILO_SeriesID=mic_i['ILO_SeriesID']
                UNESCO_SeriesID=mic_i['UNESCO_SeriesID']

        #extract the list of source years for current indicator and series:
        j_y = utils.select_dict(x1_list, {'INDICATOR_ID': i, 'MINSET_SERIES': s}, keep=True)


        #obtain the latest source year for current indicator and series:
        years = [int(float(x['SOURCE_YEAR'])) for x in j_y] 
        y = max(years)

     
        y_t = [i['SOURCE_YEAR'] for i in j_y if int(float(i['SOURCE_YEAR'])) == y][0]


        #------------------------------------------
        #Write data file name for individual series:
        #------------------------------------------
        file_name = i_label+'__'+i+'__' + s + '.csv'
        print(file_name)

        # Extract subset of records corresponding to indicator i and series s in source year y_t:
        data = utils.select_dict(x, { 'MINSET_SERIES': s, 'SOURCE_YEAR': y_t})

        # Get list of columns for the current series dataset:
        all_columns = list(data[0].keys())


        # Get the list of "dimension columns" except time (used to identify time series)

        non_TSK_columns = [ 'TIME_PERIOD',
                            'COMMENT_OBS',
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

        # print(f'non_TSK_columns: {non_TSK_columns}')
        
        TSK_columns = [x for x in all_columns if x not in non_TSK_columns]
        
        #print(f'TSK_columns: {TSK_columns}')

        TSK_sub = [x for x in TSK_columns 
                     if x not in ['INDICATOR_ID', 'INDICATOR_LABEL','INDICATOR_DESC', 'MINSET_SERIES', 'MINSET_SERIES_DESC', 'REF_AREA', 'REF_AREA_DESC'] and 
                        not x.endswith('_DESC')]

        # print(TSK_sub)

        

        # Obtain the list of Time-series identifiers (composed by TSK dimensions)
        unique_TSK_values = utils.unique_dicts(utils.subdict_list(data, non_TSK_columns, exclude=True))
        print(f"this dataset has {len(unique_TSK_values)} time series.")

        
        for u in unique_TSK_values:

            TSK_sub_values = []
            TSK_sub_descriptions = []
            for tsk in TSK_sub:
                TSK_sub_values.append(u[tsk])
                TSK_sub_descriptions.append(u[tsk+'_DESC'])
                
            u["TSK_sub_dims"] ='__'.join(TSK_sub)
            u["TSK_sub_id"] ='__'.join(TSK_sub_values)
            u["TSK_sub_desc"] = ', '.join(TSK_sub_descriptions).capitalize()


        # print(unique_TSK_values)

            

        if jdx == 0:
            with open('test/ts_keys.json', 'w') as fp:
                json.dump(unique_TSK_values, fp, indent=2)



        # Add empty column in data, which will hold the "isLatestYear" boolean
        new_data = []

        has_duplicates = []

        availability = []

        for idx, ts in enumerate(unique_TSK_values):

            # if idx!=0:
            #     continue

            # print(ts)

            TSK_sub_dims = ts['TSK_sub_dims']
            TSK_sub_id = ts['TSK_sub_id']
            TSK_sub_desc = ts['TSK_sub_desc']

            ts_1 = {k: ts[k] for k in ts.keys() if k not in ['TSK_sub_dims','TSK_sub_id','TSK_sub_desc']}


            # Number of records in time series group:
            x_ts = utils.select_dict(data, ts_1)

            

            # print(x_ts)
            N = len(x_ts)


            # Number of unique years in time series group:
            unique_years = []
            for record in utils.unique_dicts(utils.subdict_list(x_ts, ['TIME_PERIOD'])):
                unique_years.append(int(float(record['TIME_PERIOD'])))

            unique_years.sort()

            n = len(unique_years)

            if (N != n):
                has_duplicates.append(ts)

            if(len(has_duplicates)>0):
                print(f"file {f} has_duplicates")

            # Latest year available:
            y_max = max(unique_years)

            # Latest year value:

            keys = x_ts[0].keys()
            # print(f'{keys=}')

            if 'OBS_VALUE' in keys:
                # print('------------1-------------')
                value_y_max = utils.select_dict(x_ts, {'TIME_PERIOD': str(y_max)})[0]['OBS_VALUE']
            else:
                # print('------------2-------------')
                value_y_max = utils.select_dict(x_ts, {'TIME_PERIOD': str(y_max)})[0]['VALUE_CATEGORY_DESC']


            # Add ts keys and "isLatestYear"value
            for r in x_ts:
                r['sort_order'] =sort_order
                r['SDG_SeriesID'] =SDG_SeriesID
                r['ILO_SeriesID'] =ILO_SeriesID
                r['UNESCO_SeriesID'] =UNESCO_SeriesID
                r['TSK_sub_dims'] = TSK_sub_dims
                r['TSK_sub_id'] = TSK_sub_id
                r['TSK_sub_desc'] = TSK_sub_desc
                if r['TIME_PERIOD'] == str(y_max):
                    r['isLatestValue'] = True
                else:
                    r['isLatestValue'] = False

            new_data.extend(x_ts)

            #------------------------------------
            # AVAILABILITY CALCULATIONS
            #------------------------------------

            ts_availability = dict(ts)  # or orig.copy()

            ref_area = ts['REF_AREA']
            #print(ref_area)

            geo_info = utils.select_dict(geo, {'REF_AREA': ref_area}, keep=True)
            #print(geo_info[0])

            ts_availability['sort_order'] = sort_order
            ts_availability['ISO3'] = geo_info[0]['ISO3']
            ts_availability['X'] = geo_info[0]['X']
            ts_availability['Y'] = geo_info[0]['Y']
            ts_availability['UNmember'] = geo_info[0]['UNmember']
            ts_availability['GEOLEVEL'] = geo_info[0]['GEOLEVEL']
            ts_availability['GEOLEVEL_Desc'] = geo_info[0]['GEOLEVEL_Desc']
            ts_availability['SDG_REGION'] = geo_info[0]['SDG_REGION']
            ts_availability['years'] = unique_years
            ts_availability['min_year'] = min(unique_years)
            ts_availability['max_year'] = max(unique_years)
            ts_availability['N_years'] = len(unique_years)
            ts_availability['N_years_lag5'] = len([y for y in unique_years if y >= current_year - 5])
            ts_availability['N_years_lag10'] = len([y for y in unique_years if y >= current_year - 10])
            
            # Get availability stats: min(years), max (years), Number of years, Number of years after 2015
            availability.append(ts_availability)

        utils.dictList2tsv(new_data, 'series_data/' + sort_order + '__' + file_name)

        #------------------------------------------------------
        # Within current series, obtain availability summary for each country
        #------------------------------------------------------

        # Obtain the list of all countries that have data for this series
        countries =  utils.unique_dicts(utils.subdict_list(availability, ['REF_AREA']) )
        countries =  [ c['REF_AREA'] for c in countries]

        #For each country, obtain series availability (with detail for disaggregation)
        for c in countries:
        
            d = dict()

            # if c not in ['8','32']:
            #         continue
            
            # Select TS availability for this country
            data = utils.select_dict(availability, {'REF_AREA': c})

            d['INDICATOR_ID'] = data[0]['INDICATOR_ID']
            d['INDICATOR_DESC'] = data[0]['INDICATOR_DESC']
            d['MINSET_SERIES'] = data[0]['MINSET_SERIES']
            d['MINSET_SERIES_DESC'] = data[0]['MINSET_SERIES_DESC']
            d['REF_AREA'] = data[0]['REF_AREA']
            d['REF_AREA_DESC'] = data[0]['REF_AREA_DESC']
            d['ISO3'] = data[0]['ISO3']
            d['X'] = data[0]['X']
            d['Y'] = data[0]['Y']
            d['SDG_REGION'] = data[0]['SDG_REGION']
            d['UNmember'] = data[0]['UNmember']
            d['GEOLEVEL_Desc'] = data[0]['GEOLEVEL_Desc']
            d['GEOLEVEL'] = data[0]['GEOLEVEL']
            if data[0]['UNmember'] or  data[0]['REF_AREA'] in ['275','336']:
                d['Select195'] = True
            else:
                d['Select195'] = False
            d['years'] = [] 
            for ts in data:
                d['years'].extend(ts['years'])
            d['years'] = list(set(d['years']))

            d['max_year'] = max([ ts['max_year'] for ts in data ])
            d['min_year'] = min([ ts['min_year'] for ts in data ])
            d['data_points'] = sum([ ts['N_years'] for ts in data ])
            d['data_points_lag5'] = sum([ ts['N_years_lag5'] for ts in data ])
            d['data_points_lag10'] = sum([ ts['N_years_lag10'] for ts in data ])

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
            years.sort()

            age_categories = list(set(age_categories))
            sex_categories = list(set(sex_categories))


            # print(years)
            # print(age_categories)
            # print(sex_categories)

            # Age disaggregation by year:

            d_age = dict()

            # For each year ask: Is this year disaggregated by age?

            for k in age_categories:
                
                merged_years = []

                data_k = utils.select_dict(data, {'AGE': k})
            
                for record in data_k:
                    merged_years.extend(record['years'])

                d_age[k] = list(set(merged_years))

            # print(f"{d_age=}")

            # Sex disaggregation by year:

            d_sex = dict()

            for k in sex_categories:
                
                merged_years = []

                data_k = utils.select_dict(data, {'SEX': k})
            
                for record in data_k:
                    merged_years.extend(record['years'])

                d_sex[k] = list(set(merged_years))

            # print(d_sex)

            # -----------------------------
            # Which years are disaggregated by sex?

            years_d_sex = []
            
            
            for y in years:
                n = 0
                for value_sex in sex_categories:
                    # If year y has data for sext category i: 
                    if y in d_sex[value_sex]:
                        n = n + 1
                if n>1:
                    years_d_sex.append(y)
            
            # List of years that have more than 2 sex categories:
            # print(years_d_sex)

            
            # -----------------------------
            # Which years are disaggregated by age?

            years_d_age = []
            for y in years:
                n = 0
                for value_age in age_categories:
                    # If year y has data for age category i:
                    if y in d_age[value_age]:
                        n = n + 1
                if n>1:
                    years_d_age.append(y)
            
            # List of years that have more than 2 age categories:
            # print(years_d_age)

            d['disaggregated_by_age'] = years_d_age
            d['disaggregated_by_age_n'] = len(years_d_age)
            d['disaggregated_by_sex'] = years_d_sex
            d['disaggregated_by_sex_n'] = len(years_d_sex)

            d['sort_order'] = sort_order

            availability_by_series_and_country.append(d)
        
        utils.dictList2tsv(availability, 'availability_data/availability_ts_'+ sort_order + '__' + file_name)

utils.dictList2tsv(availability_by_series_and_country, 'availability_data/availability_bySeriesCountry.csv')
            
#-------------------------------------------------
# Calculate availability for the current series:
#-------------------------------------------------

# list of all series in availability file:

series =  utils.unique_dicts(utils.subdict_list(availability_by_series_and_country, ['MINSET_SERIES']) )

def availability_by_series_region (series, sdgRegion=None):

    d2 = dict()

    selector = dict()

    selector['MINSET_SERIES'] = series['MINSET_SERIES']
    selector['Select195'] = True

    series_data = utils.subdict_list(
                    utils.select_dict(availability_by_series_and_country, 
                        selector,
                        keep=True),
                        ['sort_order','INDICATOR_ID','INDICATOR_DESC','MINSET_SERIES','MINSET_SERIES_DESC']
                    )
    
    
    
    d2['sort_order'] = series_data[0]['sort_order']
    d2['INDICATOR_ID'] = series_data[0]['INDICATOR_ID']
    d2['INDICATOR_DESC'] = series_data[0]['INDICATOR_DESC']
    d2['MINSET_SERIES'] = series_data[0]['MINSET_SERIES']
    d2['MINSET_SERIES_DESC'] = series_data[0]['MINSET_SERIES_DESC']

    if sdgRegion:

        d2['SDG_REGION'] = sdgRegion

        countries_in_region = utils.select_dict(geo, {'SDG_REGION': sdgRegion, 'GEOLEVEL': '4'})

        # print(f"{countries_in_region=}")

        for c in countries_in_region:
            if c['UNmember'] or  c['REF_AREA'] in ['275','336']:
                c['c195'] = True

        d2['denominator'] = sum(x.get('c195') == True for x in countries_in_region)

    else:
        d2['SDG_REGION'] = 'All'
        d2['denominator'] = 195
    
    # Initialize values:


    if sdgRegion:
        selector['SDG_REGION']= sdgRegion

    # print(f"{selector=}")

    data_s = utils.select_dict(availability_by_series_and_country, 
                                selector,
                                keep=True)
    
    d2['N_countries'] = 0
    d2['N_countries_lag10'] = 0
    d2['N_countries_lag5'] = 0
    d2['N_countries_sex'] = 0
    d2['N_countries_age'] = 0
    
    if(len(data_s)>0):
    
        for idx, c in enumerate(data_s):

            if c['data_points'] > 0:
                d2['N_countries'] += 1
            if c['data_points_lag10'] > 0:
                d2['N_countries_lag10'] += 1
            if c['data_points_lag5'] > 0:
                d2['N_countries_lag5'] += 1
            if c['disaggregated_by_age_n'] > 0:
                d2['N_countries_age'] += 1
            if c['disaggregated_by_sex_n'] > 0:
                d2['N_countries_sex'] += 1
     
    return d2

sdgRegions =  [ None,
    'Central and Southern Asia',
    'Europe and Northern America',
    'Northern Africa and Western Asia',
    'Sub-Saharan Africa',
    'Latin America and the Caribbean',
    'Australia and New Zealand',
    'Eastern and South-Eastern Asia',
    'Oceania, exc. Australia and New Zealand'
    ]

# for sdgRegion in sdgRegions:

availability_by_series = [] 

for sdgRegion in sdgRegions:
    
    for s in series:

        # Keep records for the current series, only 193 Member States plus 2 Observer States
        data_s = utils.select_dict(availability_by_series_and_country, {'MINSET_SERIES': s['MINSET_SERIES'], 'Select195': True}, keep=True)

        asr = availability_by_series_region (s, sdgRegion)
        if asr:
            availability_by_series.append(asr)
            
#------------------------------------------------------

# utils.dictList2tsv(availability_by_series, 'availability_data/availability_bySeries_'+utils.camel_case(sdgRegion)+'.csv')
utils.dictList2tsv(availability_by_series, 'availability_data/availability_bySeries.csv')
        
        