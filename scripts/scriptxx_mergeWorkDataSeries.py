import utils
import csv
import json

data_100 = []

with open('series_data_rearranged/100__I_1__I98ba1fd8__S997d1fdb.csv', newline='', encoding='utf-8', errors='ignore') as f:
    x = csv.DictReader(f, delimiter=',', quotechar='"')
    for row in x:
        data_100.append(dict(row))

data_400 = []

with open('series_data_rearranged/400__I_2__Ic62a19e8__Sad2e1d29.csv', newline='', encoding='utf-8', errors='ignore') as f:
    x = csv.DictReader(f, delimiter=',', quotechar='"')
    for row in x:
        data_400.append(dict(row))



for k in data_100[0].keys():
    print(k)

    
for k in data_400[0].keys():
    print(k)

common_columns = [
'REF_AREA',
'REF_AREA_DESC',
'ISO3',
'SDG_REGION',
'REPORTING_TYPE',
'REPORTING_TYPE_DESC',
'TIME_PERIOD'
]


geo_time = utils.subdict_list(data_100, common_columns)
geo_time.extend(utils.subdict_list(data_400, common_columns))

geo_time = utils.unique_dicts(geo_time)

print(len(geo_time))

with open('geo_time.json', 'w') as fout:
    json.dump(geo_time, fout,  indent=4)

value_keys_100 = [k for k in data_100[0].keys() if k.startswith('OBS_VALUE')]
value_keys_400 = [k for k in data_400[0].keys() if k.startswith('OBS_VALUE')]

print(f"{value_keys_100=}")
merged_file = []
for gt in geo_time:
    d = gt.copy()
    for d1 in data_100:
        if d1['REF_AREA'] != gt['REF_AREA'] or d1['TIME_PERIOD'] != gt['TIME_PERIOD']:
            continue


        d['INDICATOR_LABEL_100'] = d1['INDICATOR_LABEL']
        d['INDICATOR_ID_100'] = d1['INDICATOR_ID']
        d['INDICATOR_DESC_100'] = d1['INDICATOR_DESC']
        d['MINSET_SERIES_100'] = d1['MINSET_SERIES']
        d['MINSET_SERIES_DESC_100'] = d1['MINSET_SERIES_DESC']
        d['X'] = d1['X']
        d['Y'] = d1['Y']
        for k100 in value_keys_100:
            d[k100.replace('OBS_VALUE__','OBS_VALUE__100__')] = d1[k100]
    for d2 in data_400:
        if d2['REF_AREA'] != gt['REF_AREA'] or d2['TIME_PERIOD'] != gt['TIME_PERIOD']:
            continue
        if not d['X']:
            d['X'] = d2['X']
            d['Y'] = d2['Y']
        
        for k400 in value_keys_400:
            d[k400.replace('OBS_VALUE__','OBS_VALUE__400__')] = d2[k400]

    merged_file.append(d)

utils.dictList2csv(merged_file, 'merged__file.csv')

