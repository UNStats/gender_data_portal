import utils
from os import listdir
from os.path import isfile, join

from openpyxl import Workbook, load_workbook


# 1. Open the 'catalog_series_old_and_new.xlsx' file

catalog_mapping = utils.xlsx2dict('master_data/catalog_series_old_and_new.xlsx',0)
print(catalog_mapping[0])
print('---')

for cm in catalog_mapping:
    if cm['OLD_INDICATOR_ID']:
        cm['OLD_INDICATOR_ID'] = str(int(float(cm['OLD_INDICATOR_ID'])))
    if cm['OLD_MINSET_SERIES']:
        cm['OLD_MINSET_SERIES'] = str(int(float(cm['OLD_MINSET_SERIES'])))

# 2. Open each file in the 'source_data_old' folder

old_datafiles = [f for f in listdir('source_data_old/') if isfile(join('source_data_old/', f))]

for fdx, f in enumerate(old_datafiles):

    # if fdx !=0:
    #     continue

    x = utils.xlsx2dict('source_data_old/'+ f, 0)
    print(x[0])
    print('---')



# 3. Replace old indicator and series IDs with new indicator and series IDs

    old_columns = utils.unique_dicts(utils.subdict_list(x, ['INDICATOR_ID', 'MINSET_SERIES']))
    print(old_columns)
    
    new_x = []
    for y in old_columns:

        old_indicator_id = y['INDICATOR_ID']
        old_series_id = y['MINSET_SERIES']

        new_record = utils.select_dict(catalog_mapping,{'OLD_INDICATOR_ID': old_indicator_id, 'OLD_MINSET_SERIES': old_series_id})[0]
        print(new_record)
        print('---')

        indicator_label = str(new_record['INDICATOR_LABEL'])
        indicator_id = str(new_record['INDICATOR_ID'])

        for record in x:
            if record['MINSET_SERIES'] == old_series_id:
                d = dict()
                d['INDICATOR_LABEL'] = indicator_label
                for k in record.keys():
                    if k == 'INDICATOR_NUM':
                        continue
                    elif k == 'INDICATOR_ID':
                        d[k] = str(new_record[k])
                    elif k == 'MINSET_SERIES':
                        d[k] = str(new_record[k])
                    elif k in new_record.keys():
                        d[k] = new_record[k]
                    else:
                        d[k] = record[k]
                new_x.append(d)


# 4. Save each modified file in the 'source_data' folder

    wb = Workbook()
    

    sheet = wb.active

    headers = list(new_x[0].keys())
    sheet.append(headers)

    print(f"{len(new_x)=}")

    for record in new_x:
        sheet.append(list(record.values()))

    wb.save('source_data/Ind_'+ indicator_label.replace('.','_')+ '__' + indicator_id+'_data.xlsx')
