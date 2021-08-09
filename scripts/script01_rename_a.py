import utils
from os import listdir
from os.path import isfile, join

# List of old source data files
datafiles = [f for f in listdir('source_data_old/') if isfile(join('source_data_old/', f))]


catalog_series_old = []

for f in datafiles:

    print(f'file: {f}')

    # if f not in ['01_data.xlsx']:
    #      continue
    
    # if not f.startswith('Qual'):
    #     continue
    x = utils.xlsx2dict('source_data_old/'+ f, 0)

    # Read source data file:
    catalog_series_old.extend(
        utils.unique_dicts(
            utils.subdict_list(x, ['INDICATOR_ID', 'INDICATOR_DESC', 'MINSET_SERIES', 'MINSET_SERIES_DESC']
                )
            )
        )

utils.dictList2csv(catalog_series_old, 'test/catalog_series_old.csv')

## Note:
## After this script is run, the 'catalog_series_old_and_new.xlsx' file is 
## manually edited 
