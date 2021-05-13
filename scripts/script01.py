#------------------
# Create metadata_minset.json
#-------------------

import utils
import json



# Read minset_indicators_catalog
minset_indicators_catalog = utils.xlsx2dict('master_data/MINSET_Indicators.xlsx', 'CL_INDICATORS')

# Read minset_series_catalog
minset_series_catalog = utils.xlsx2dict('master_data/MINSET_series_catalog.xlsx', 'CL_SERIES')

# Read metadata
metadata_info = utils.xlsx2dict('master_data/Metadata.xlsx', 'Metadata')

# print(minset_indicators_catalog[0])
# print(minset_series_catalog[0])
print(metadata_info[0])

metadata = []

for i in minset_indicators_catalog:
    
    for k in i.keys():
        if i[k] == 'nan':
            i[k] = None
    
    i['series'] = []

    for s in minset_series_catalog:
        
        # if s['MINSET_SERIES'] != "72":
        #     continue

        series_detail = dict()
        if s['INDICATOR_ID'] == i['INDICATOR_ID']:
            series_detail['MINSET_SERIES'] = s['MINSET_SERIES']
            series_detail['MINSET_SERIES_DESC'] = s['MINSET_SERIES_DESC']
            series_detail['Retired'] = s['Retired']
            series_detail['SDG_SeriesID'] = s['SDG_SeriesID']

            series_detail['Metadata'] = []

            metadata_i_s = utils.select_dict(metadata_info, 
                                            {'INDICATOR_ID': i['INDICATOR_ID'], 
                                            'MINSET_SERIES': s['MINSET_SERIES']}
                                            )


            for m in metadata_i_s:
                metadata_dict = {}
                metadata_dict['METADATA_CATEGORY']= m['METADATA_CATEGORY']
                metadata_dict['METADATA_CATEGORY_DESC']= m['METADATA_CATEGORY_DESC']
                metadata_dict['METADATA_DESCRIPTION']= m['METADATA_DESCRIPTION']
                metadata_dict['METADATA_SOURCE']= m['METADATA_SOURCE']
                metadata_dict['SOURCE_YEAR']= m['SOURCE_YEAR']

                series_detail['Metadata'].append(metadata_dict)

            i['series'].append(series_detail)    

    metadata.append(i)




with open('master_data/metadata_minset.json', 'w') as fout:
    json.dump(metadata, fout,  indent=4)