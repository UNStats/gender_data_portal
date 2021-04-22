import utils
import json
from os import listdir
from os.path import isfile, join

# # Read minset_indicators_catalog
# minset_indicators_catalog = utils.xlsx2dict('master_data/MINSET_Indicators.xlsx', 'CL_INDICATORS')

datafiles = [f for f in listdir('source_data/') if isfile(join('source_data/', f))]

series_source = []
for i in datafiles:
    x = utils.unique_dicts(
            utils.subdict_list(
                utils.xlsx2dict('source_data/'+ i, 0), ['INDICATOR_ID','MINSET_SERIES']
            )
        )
    for j in x:
        j['file'] = i
    
    print(i)

    series_source.extend(x)

print(series_source)


with open('master_data/series_sources.json', 'w') as fout:
    json.dump(series_source, fout,  indent=4)