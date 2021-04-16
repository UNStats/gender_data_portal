import utils
import json

# Read minset_indicators_catalog

minset_indicators_catalog = utils.xlsx2dict('master_data/MINSET_Indicators.xlsx', 'CL_INDICATORS')

minset_series_catalog = utils.xlsx2dict('master_data/MINSET_series_catalog.xlsx', 'CL_SERIES')

print(minset_indicators_catalog[0])
print(minset_series_catalog[0])

metadata = []

for i in minset_indicators_catalog:
    
    for k in i.keys():
        if i[k] == 'nan':
            i[k] = None
    
    i['series'] = []


    for s in minset_series_catalog:
        series_detail = dict()
        if s['INDICATOR_ID'] == i['INDICATOR_ID']:
            series_detail['MINSET_SERIES'] = s['MINSET_SERIES']
            series_detail['MINSET_SERIES_DESC'] = s['MINSET_SERIES_DESC']
            series_detail['Retired'] = s['Retired']
            series_detail['SDG_SeriesID'] = s['SDG_SeriesID']

            for k in series_detail.keys():
                if series_detail[k] == 'nan':
                    series_detail[k] = None

            i['series'].append(series_detail)
    metadata.append(i)




with open('master_data/metadata_minset.json', 'w') as fout:
    json.dump(metadata, fout,  indent=4)