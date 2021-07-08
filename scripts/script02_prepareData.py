import utils
import utils2
from os import listdir
from os.path import isfile, join

# PROCESSING STEPS

# 1. Read / Validate Indicators catalog

# Read minset_indicators_catalog
minset_indicators_catalog = utils.xlsx2dict('master_data/MINSET_Indicators.xlsx', 'CL_INDICATORS')
#print(minset_indicators_catalog[0])

# Read geographic areas catalog
geo = utils.xlsx2dict('master_data/CL_AREA.xlsx', 0)
#print(geo)

# List of source data files
datafiles = [f for f in listdir('source_data/') if isfile(join('source_data/', f))]
#print(datafiles)

series_source = []

for f in datafiles:

    #print(f'file: {f}')

    if f != '10_data.xlsx':
         continue

    # if not f.startswith('Qual'):
    #     continue

    # Read source data file:
    x = utils.xlsx2dict('source_data/'+ f, 0)
    # print(x[0]) 

    # Ensure column names are uppercase
    x = utils2.col_names_to_uppercase(x)

    #-----------------------------------
    # List of unique indicator-series-sourceYear combinations
    #-----------------------------------
    x1_list = utils.unique_dicts( utils.subdict_list(x, ['INDICATOR_ID','MINSET_SERIES', 'SOURCE_YEAR'] ) )
    #print(x1_list)

    #-----------------------------------
    # List of unique indicator-series combinations:
    #-----------------------------------
    x2_list = utils.unique_dicts( utils.subdict_list(x1_list, ['INDICATOR_ID','MINSET_SERIES'] ) )
    #print(x2_list)

    #-----------------------------------
    # Trasformations on individual series
    #-----------------------------------

    for j in x2_list:

        #current indicator and series id's:
        i = j['INDICATOR_ID']
        s = j['MINSET_SERIES']

        #extract the list of source years for current indicator and series:
        j_y = utils.select_dict(x1_list, {'INDICATOR_ID': i, 'MINSET_SERIES': s}, keep=True)
        print(j_y)

        #obtain the latest source year for current indicator and series:
        years = [int(float(x['SOURCE_YEAR'])) for x in j_y] 
        y = max(years)
     
        y_t = [i['SOURCE_YEAR'] for i in j_y if int(float(i['SOURCE_YEAR'])) == y][0]
        print(y_t)

        #------------------------------------------
        #Write data file name for individual series:
        #------------------------------------------
        file_name = 'ind_'+i+'__series_' + s + '.csv'
        print(file_name)

        # Extract subset of records corresponding to indicator i and series s in source year y_t:
        data = utils.select_dict(x, { 'INDICATOR_ID': i, 'MINSET_SERIES': s, 'SOURCE_YEAR': y_t})

        # Get list of columns for the current series dataset:
        all_columns = list(data[0].keys())
        print(all_columns)

        # Get the list of "dimension columns" except time (used to identify time series)

        non_TSK_columns = [ 'TIME_PERIOD',
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

        print(f'non_TSK_columns: {non_TSK_columns}')
        
        TSK_columns = [x for x in all_columns if x not in non_TSK_columns]
        
        print(f'TSK_columns: {TSK_columns}')

        # Obtain the list of Time-series identifiers (composed by TSK dimensions)
        unique_TSK_values = utils.unique_dicts(utils.subdict_list(x, non_TSK_columns, exclude=True))
        print(f"this dataset has {len(unique_TSK_values)} time series.")
        print(unique_TSK_values[0])

        has_duplicates = []

        for ts in unique_TSK_values:

            # Number of records in time series group:
            x_ts = utils.select_dict(x, ts )
            N = len(x_ts)

            # Number of unique years in time series group:
            unique_years = []
            for i in utils.unique_dicts(utils.subdict_list(x_ts, ['TIME_PERIOD'])):
                unique_years.append(int(float(i['TIME_PERIOD'])))

            print(f'unique_years: {unique_years}')
            unique_years.sort()

            print(unique_years)
            n = len(unique_years)

            if (N != n):
                has_duplicates.append(ts)

            if(len(has_duplicates)>0):
                print(f"file {f} has_duplicates")

            # Latest year available:
            y_max = max(unique_years)
            print(y_max)

            # # Latest year value:
            # value_y_max = utils.subdict_list(x_ts, {'TIME_PERIOD': str(y_max)})[0]['OBS_VALUE']
            # print(value_y_max)
            # print('------')



# # # 2. Read / Validate Indicators metadata



# # # 3. Read / Validate Indicators source data
# # #    - All column names are uppercase





# # #    - No duplicate records for same indicator/series/source year
# # #    - No empty values
# # #    - No empty dimensions
# # # 4. Create formatted indicators file
# # #    - Take only latest source year
# # #    - Idetify distinct time series
# # #      - catalog of dimensions / values
# # #      - data availability
