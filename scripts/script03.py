import utils
import json
from os import listdir
from os.path import isfile, join

# Read csv file

f = utils.tsv2dictlist('series_data/ind_1__series_72.csv')
print(f[0])

# Get the list of "columns"

all_columns = f[0].keys()

print(all_columns)

# Get the list of "dimension columns" (those thare are used to identify time series)

non_TSK_columns = ['TIME_PERIOD', 
                   'TIME_DETAIL', 
                   'OBS_VALUE', 
                   'NATURE', 'NATURE_DESC', 
                   'COMMENT_OBS', 
                   'SOURCE_YEAR']

# TSK_columns = [x for x in all_columns if x not in non_TSK_columns]
# print(TSK_columns)

unique_TSK_values = utils.unique_dicts(utils.subdict_list(f, non_TSK_columns, exclude=True))

#for ts in unique_TSK_values:
ts = unique_TSK_values[12]
x = utils.select_dict(f, ts)

print(x)

# For each combination of time series key, identify the latest year