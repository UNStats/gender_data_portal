import utils
import json
from os import listdir
from os.path import isfile, join

# # Read minset_indicators_catalog
# minset_indicators_catalog = utils.xlsx2dict('master_data/MINSET_Indicators.xlsx', 'CL_INDICATORS')

datafiles = [f for f in listdir('source_data/') if isfile(join('source_data/', f))]

series_source = []

for i in datafiles:

    x = utils.xlsx2dict('source_data/'+ i, 0)

    x_list = utils.unique_dicts( utils.subdict_list(x, ['INDICATOR_ID','MINSET_SERIES'] ) )

    for j in x_list:
        # j['file'] = i

        file_name = 'ind_'+j['INDICATOR_ID']+'__series_' + j['MINSET_SERIES'] + '.csv'

        data = utils.select_dict(x, { 'INDICATOR_ID': j['INDICATOR_ID'], 'MINSET_SERIES': j['MINSET_SERIES']})

        utils.dictList2tsv(data, 'series_data/' + file_name)
        
    print(i)
