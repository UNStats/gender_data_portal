
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
    index = []
    
    for k in ['INDICATOR_LABEL',
               'INDICATOR_ID', 
               'INDICATOR_DESC', 
               'MINSET_SERIES', 
               'MINSET_SERIES_DESC', 
               'REF_AREA', 
               'REF_AREA_DESC',
               'ISO3', 
               'X', 
               'Y', 
               'SDG_REGION', 
               'REPORTING_TYPE',
               'REPORTING_TYPE_DESC',
               'SOURCE_YEAR',
               'UNIT_MEASURE',
               'UNIT_MEASURE_DESC',
               'TIME_PERIOD'
               ]:
        if k in x[0].keys():
            index.append(k)

    val_columns = []

    for k in ['OBS_VALUE', 'VALUE_CATEGORY_DESC', 'TIME_DETAIL','NATURE', 'COMMENT_OBS', 'isLatestValue']:
        if k in x[0].keys():
            val_columns.append(k)

    x2 = df.pivot(index = index, columns=['TSK_sub_id','TSK_sub_desc'], values=val_columns)

    x2 = x2.reset_index(level=index)

    x2.to_excel("test/test.xlsx") 
    x3 = x2.to_dict('records')

    keys = []
    aliases = []
    for k,v in x3[0].items():
        if k[0] in index:
            key = k[0]
            alias = k[0]
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