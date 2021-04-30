

# This module allows to make HTTP requests:
import requests

# This module is required to perform file-related I/O operations (eg. file reading/writing)
import io

# This module is used to create time stamps
import time

# The pandas module provides data analysis and manipulation functionality 
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

import utils_arcgis

from arcgis.gis import GIS
# Establish ArcGIS connection
online_username, gis = utils_arcgis.connect_to_arcGIS()
print(gis)

#--------------------------------------------------------------------
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 50)
#--------------------------------------------------------------------

#chose the indicator and series for this data
set_indicator = "1"
set_series = "72"


#--------------------------------------------------------------------
#Country Layer to join Data to
un_country_layer_id = '25c570fd7f074471b98f5108792aaa84'
#--------------------------------------------------------------------

#Statistic Variable
statistic_field = "OBS_VALUE"

#--------------------------------------------------------------------

sdg_key = "ind_" + str(set_indicator) + "__series_" + str(set_series)

#create timestamp to allow rerunning
stamp = str(time.time() * 1000).replace(".","")

print('Processing SDG Information for', sdg_key, stamp)
#--------------------------------------------------------------------
un_github_data = "https://github.com/UNStats/gender_data_portal"
un_github_raw = un_github_data.replace('github.com', 'raw.githubusercontent.com')
un_github_csv = un_github_raw +"/main/series_data/" + sdg_key + ".csv"
un_github_metadata = un_github_raw +"/main/master_data/metadata_minset.json"



#---------------------
print(un_github_metadata)
metadata = requests.get(un_github_metadata)
series_metadata = metadata.json()
#print(series_metadata[0])
#---------------------

indicator_properties = list(
    series_metadata[0].keys())

print(indicator_properties)

series_properties = list(
    series_metadata[0]['series'][0].keys())

print(series_properties)

#------------------------

for m in series_metadata:
    if m['INDICATOR_ID'] != set_indicator:
        continue

    indicator_dict = m

    for s in m['series']:
        if s['MINSET_SERIES'] != set_series:
            continue

        series_dict = s

    del indicator_dict['series']


#print(indicator_dict)
#-----
#print(series_dict)

#-----------------------------------------------------

print(un_github_csv)
tabular = requests.get(un_github_csv).content
tabular_df = pd.read_csv(io.StringIO(tabular.decode('utf-8')), sep='\t')
print(tabular_df.head(5))

#----------------------------------------------------

tabular_df['latitude'] = tabular_df['Y']
tabular_df['longitude'] = tabular_df['X']
print(tabular_df.info())

#-----------------------------------------------------

csv_file = 'series_data_prepared/' + sdg_key + "_" + stamp +'.csv'
tabular_df.to_csv(csv_file, encoding="utf-8", index=False)
csv_item = gis.content.add({}, csv_file)
print(csv_item)
publish_parameters={'type': 'csv', 'name': sdg_key, 'locationType': 'coordinates', 'longitudeFieldName': 'longitude' , 'latitudeFieldName': 'latitude'}
point_layer = csv_item.publish(None, publish_parameters)
point_layer

#-----------------------------------------------------
un_country_item = gis.content.get(un_country_layer_id)
print(un_country_item)

un_country_layer = un_country_item.layers[0]
un_country_features = un_country_layer.query(where='1=1', as_df=True)
un_country_features

countries_df = pd.merge(un_country_features, tabular_df, left_on='ISO3', right_on='ISO3')
countries_df.drop(columns=['FID','POP2005', 'REGION', 'LON', 'LAT','Shape__Area','Shape__Length'], inplace=True)
countries_df.info()

feature_layer_key = sdg_key + "_" + stamp
print(feature_layer_key)
published_polygon_layer = countries_df.spatial.to_featurelayer(feature_layer_key)

print(published_polygon_layer)

#-------------------------------------------------------

series_card = dict()
series_desc = series_dict['MINSET_SERIES_DESC'].replace('%', 'percent').replace(',', ' ').replace('/', ' ')
title = 'Indicator ' + indicator_dict['INDICATOR_DESC'] + ': ' + series_desc
series_card['title'] = (title[:250] + '..') if len(title) > 250 else title
layer_title = 'Indicator_' + \
    indicator_dict['INDICATOR_ID'] + '__Series_' + series_dict['MINSET_SERIES']
series_card['layer_title'] = layer_title[:89] if len(layer_title) > 88 else layer_title  
series_card['snippet'] = series_card['title']

series_card['description'] =  \
        '<div style="background-color: #f78b33; color:#fff; padding: 15px">' + \
        '<ul style="list-style: none;">' + \
        '<li><strong> Series Name:</strong> ' + series_desc + '</li>' + \
        '<li><strong>Series Code:</strong> ' + series_dict['MINSET_SERIES'] + '</li>' + \
        '</ul>' + \
        '</div>' + \
        '<div style="background-color: #f4f4f4; padding: 15px">' + \
        '<p> This dataset is the part of the Minimum Set of Gender Indicators compiled ' + \
        'through the United Nations Statistics Division.' + \
        '</p>' + \
        '<p><strong>Indicator ' + indicator_dict['INDICATOR_ID'] + ': </strong>' + indicator_dict['INDICATOR_DESC'] + \
        '</p>' +  \
        '<p><em>For more information on the compilation methodology of this dataset, ' +\
        ' see <a href="https://unstats.un.org/" target="_blank">https://unstats.un.org/' + \
        '</a></em></p>' + \
        '</div>'


series_card['tags'] = ['Minimum Gender Data Set']

print(series_card)

#-----------------

thumbnail = 'https://raw.githubusercontent.com/UNStats/gender_data_portal/main/thumbnails/ww2020_thumbnail.png'
point_layer.update(series_card,thumbnail= thumbnail)

#-----------------

series_title = series_card["title"]
if len(series_title) > 256:
    series_title = series_title[:250] + '..'
