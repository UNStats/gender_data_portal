import utils
import json
from os import listdir
from os.path import isfile, join

# # Read minset_indicators_catalog
# minset_indicators_catalog = utils.xlsx2dict('master_data/MINSET_Indicators.xlsx', 'CL_INDICATORS')



datafiles = [f for f in listdir('source_data/') if isfile(join('source_data/', f))]

for i in datafiles:
    x = utils.subdict_list(utils.xlsx2dict('source_data/'+ i, 'MinSet'), ['INDICATOR_ID','MINSET_SERIES'])
    print(x)