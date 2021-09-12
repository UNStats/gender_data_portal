
import utils
import utils2
import json
from os import listdir
from os.path import isfile, join

# List of availability_ts files
datafiles = [f for f in listdir('availability_data/') if isfile(join('availability_data/', f))]

columns_selector = ['INDICATOR_LABEL', 'INDICATOR_ID', 'INDICATOR_DESC', 
                    'MINSET_SERIES', 'MINSET_SERIES_DESC', 
                    'TSK_sub_dims', 'TSK_sub_id', 'TSK_sub_desc', 
                    'sort_order']

ts_catalog = []


for fdx, f in enumerate(datafiles):

    if f in ['availability_bySeries.csv','availability_bySeriesCountry.csv', 
             'availability_bySeriesCountry_long.csv',
             'availability_bySeriesCountry_wide_disaggregatedAge.csv',
             'availability_bySeriesCountry_wide_disaggregatedSex.csv',
             'availability_bySeriesCountry_wide.csv']:
        continue

    # if f != 'availability_ts_100__I_1__I98ba1fd8__S997d1fdb.csv':
    #     continue

    print(f"\nfile: {f}")

    x = utils.unique_dicts(utils.subdict_list(utils.tsv2dictlist('availability_data/'+ f),columns_selector))

    ts_catalog.extend(x)

utils.dictList2csv(ts_catalog, 'master_data/ts_catalog.csv')
