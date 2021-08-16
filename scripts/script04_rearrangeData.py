
import utils
import utils2
import json
from os import listdir
from os.path import isfile, join

import pandas as pd

# 1. Read series data into pandas data frame


# List of series data files
datafiles = [f for f in listdir('series_data/') if isfile(join('series_data/', f))]

column_aliases = []


for fdx, f in enumerate(datafiles):

    print(f"\nfile: {f}\n")

    a = dict()

    # if fdx != 0:
    #     continue

    # if f != 'ind_I_1__98ba1fd8__series_0db81b0d.csv':
    #     continue

    # f_components = f.split('__')
    # print(f_components)

    # if 'q' in f_components[0]:
    #     continue

    x = utils.tsv2dictlist('series_data/'+f)
    print(x[0])
    df = pd.DataFrame(x)

    #print(df)
    index = dict()

    common_columns = {'INDICATOR_LABEL': 'Indicator Label',
               'INDICATOR_ID': 'Indicator ID', 
               'INDICATOR_DESC': 'Indicator Description', 
               'MINSET_SERIES': 'Series ID', 
               'MINSET_SERIES_DESC': 'Series Description', 
               'REF_AREA': 'Geographic Area ID', 
               'REF_AREA_DESC': 'Geographic Area Name',
               'ISO3': 'ISO Alpha-3 Code', 
               'X': 'Longitude', 
               'Y': 'Latitude', 
               'SDG_REGION': 'SDG Region', 
               'REPORTING_TYPE': 'Reporting Type Code',
               'REPORTING_TYPE_DESC': 'Reporting Type Description',
               'SOURCE_YEAR': 'Source Year',
               'UNIT_MEASURE': 'Unit of Measurement Code',
               'UNIT_MEASURE_DESC': 'Unit of Measurement Description',
               'TIME_PERIOD': 'Year'
    }
    
    for k,v in common_columns.items():
        if k in x[0].keys():
            index[k] = v

    value_columns = {
        'OBS_VALUE': 'Value', 
        'VALUE_CATEGORY_DESC': 'Category', 
        'TIME_DETAIL': 'Time Detail',
        'NATURE': 'Nature', 
        'COMMENT_OBS': 'Comment', 
        'isLatestValue': 'Is Latest Value'
    }

    val_columns = dict()

    for k,v in value_columns.items():
        if k in x[0].keys():
            val_columns[k] = v

    x2 = df.pivot(index = list(index.keys()), 
                  columns=['TSK_sub_id','TSK_sub_desc'], 
                  values=list(val_columns.keys()))

    x2 = x2.reset_index(level=list(index.keys()))

    x2.to_excel("test/test.xlsx") 
    x3 = x2.to_dict('records')

    keys = []
    aliases = []
    for k,v in x3[0].items():
        if k[0] in index:
            key = k[0]
            alias = index[k[0]]
        elif k[0] in val_columns:
            key = k[0]+'__'+k[1]
            if k[2]:
                alias = val_columns[k[0]]+ ': ' + k[2]
            else:
                alias = val_columns[k[0]]
        else:
            key = k[0]+'__'+k[1]
            alias = k[0]+ ': ' + k[2]

        if key.endswith('__'):
            key = key[:-2]
        
        keys.append(key)
        aliases.append(alias)

    x4 = []
    for rdx, record in enumerate(x3):
        d = dict()
        if rdx == 0:
            
            print(record)
        for idx, v in enumerate(record.values()):
            if str(v) == 'nan':
                d[keys[idx]] = None
            else:
                d[keys[idx]] = v
        x4.append(d)

    utils.dictList2csv(x4,'series_data_rearranged/'+ f)

    a['file'] = f
    for kdx, k in enumerate(keys):
        a[k] = aliases[kdx]

    column_aliases.append(a)

with open('master_data/column_aliases.json', 'w') as fout:
    json.dump(column_aliases, fout,  indent=4)

# 2. Rearrage in time-series