import utils
import json
from os import listdir
from os.path import isfile, join

# # Read minset_indicators_catalog
# minset_indicators_catalog = utils.xlsx2dict('master_data/MINSET_Indicators.xlsx', 'CL_INDICATORS')

datafiles = [f for f in listdir('source_data/') if isfile(join('source_data/', f))]

print(datafiles)

series_source = []

for f in datafiles:

    # if f != 'Qual_1_data.xlsx':
    #     continue

    if not f.startswith('Qual'):
        continue

    x = utils.xlsx2dict('source_data/'+ f, 0)

    # list of unique indicator-series-sourceYear combinations:
    x1_list = utils.unique_dicts( utils.subdict_list(x, ['INDICATOR_ID','MINSET_SERIES', 'SOURCE_YEAR'] ) )

    # list of unique indicator-series combinations:
    x2_list = utils.unique_dicts( utils.subdict_list(x1_list, ['INDICATOR_ID','MINSET_SERIES'] ) )

    print(f'file: {f}')

    for j in x2_list:


        print(j)

        i = j['INDICATOR_ID']
        s = j['MINSET_SERIES']


        j_y = utils.select_dict(x1_list, {'INDICATOR_ID': i, 'MINSET_SERIES': s}, keep=True)

        print(j_y)

        years = [int(float(x['SOURCE_YEAR'])) for x in j_y] 
        
        y = max(years)

        y_t = [x['SOURCE_YEAR'] for x in j_y if int(float(x['SOURCE_YEAR'])) == y][0]

        file_name = 'ind_'+i+'__series_' + s + '.csv'

        print(file_name)

        data = utils.select_dict(x, { 'INDICATOR_ID': i, 'MINSET_SERIES': s, 'SOURCE_YEAR': y_t})

        utils.dictList2tsv(data, 'series_data/' + file_name)
        
        print('----')