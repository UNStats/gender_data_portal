# This module allows to render HTML code snippets created in this notebook:
from IPython.core.display import display, HTML

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
online_username, gis_online_connection = utils_arcgis.connect_to_arcGIS()
print(gis_online_connection)

# Specify the realease and the goal, target, indicator and series IDs:
release = '2020.Q1.G.02'
set_goal = 10
set_target = 1
set_indicator = 1
set_series = "SI_HEI_TOTL"

#choose language - 'english', 'french'
card_lang = 'english'

#is this an update or a new file (set to true if existing item - you must provide the item you are updating if update=True)
update = False
update_item_point = '87b8d2a571b8447ba23de13971a6ff04'
update_item_polygon = '15bc144b6a26420bb4aa69a3cfd8db47'

#Country Layer to join the SDG Data to
un_country_layer_id = '69f1e70901a54ba19fb21d918b26e66a'

#Statistic Variable
statistic_field = "latest_value"

#Set the Quantile Code from the series you are interested in
#Quantile Code = "_T"

#-------------------

sdg_key = "Indicator_" + str(set_goal) + "_" + str(set_target) + "_" + str(set_indicator) + "__Series_" + str(set_series)

#create timestamp to allow rerunning
stamp = str(time.time() * 1000).replace(".","")

print('Processing SDG Information for', sdg_key, stamp)

#-------------------

un_github_data = "https://github.com/UNStats/fis4sdg_2020"
un_github_raw = un_github_data.replace('github.com', 'raw.githubusercontent.com')
un_github_csv = un_github_raw +"/master/data/processed/" + release + "/" + sdg_key + ".csv"
un_github_metadata = un_github_raw +"/master/data/external/metadata_" + release + ".json"


#---------------------
print('----------CELL 01--------')
print(un_github_metadata)
metadata = requests.get(un_github_metadata)
series_metadata = metadata.json()
series_metadata[1]

#---------------------
print('----------CELL 02--------')

goal_properties = list(series_metadata[0].keys())

target_properties = list(series_metadata[1]['targets'][0].keys())

indicator_properties = list(
    series_metadata[1]['targets'][0]['indicators'][0].keys())

series_properties = list(
    series_metadata[1]['targets'][0]['indicators'][0]['series'][0].keys())

print(goal_properties)
print(target_properties)
print(indicator_properties)
print(series_properties)
#---------------------
print('----------CELL 03--------')

goal_dict = series_metadata[set_goal - 1]
target_dict = goal_dict['targets'][set_target - 1]
indicator_dict = target_dict['indicators'][set_indicator - 1]

series_index = 0
for i in range(len(indicator_dict['series'])-1):
    print(indicator_dict['series'][i]['code'])
    if set_series == indicator_dict['series'][i]['code']:
        series_index = i
        
series_dict = indicator_dict['series'][series_index]

print(goal_dict)
print(target_dict)
print(indicator_dict)
print(series_dict)
#---------------------
print('----------CELL 04--------')

print(un_github_csv)
tabular = requests.get(un_github_csv).content
tabular_df = pd.read_csv(io.StringIO(tabular.decode('utf-8')), sep='\t')

#filter out the quantile dimension
#tabular_df = tabular_df.where([tabular_df['quantile_code'] = '_T'])
print(tabular_df.head())

#---------------------
print('----------CELL 05--------')

if update:
    point_layer = gis.content.get(update_item_points)
    existing_features = point_layer.layers[0]
    existing_features.manager.truncate()
    #TODO:  Update the Point Layer with the CSV File
else:
    csv_file = 'test/' + sdg_key + "_" + stamp +'.csv'
    tabular_df.to_csv(csv_file, encoding="utf-8", index=False)
    csv_item = gis_online_connection.content.add({}, csv_file)
    csv_item
    publish_parameters={'type': 'csv', 'name': sdg_key, 'locationType': 'coordinates', 'longitudeFieldName': 'X' , 'latitudeFieldName': 'Y'}
    point_layer = csv_item.publish(None, publish_parameters)
point_layer

#---------------------
print('----------CELL 06--------')

un_country_item = gis_online_connection.content.get(un_country_layer_id)
print(un_country_item)

#---------------------
print('----------CELL 07--------')
un_country_layer = un_country_item.layers[0]
un_country_features = un_country_layer.query(where='1=1', as_df=True)
print(un_country_features)


#---------------------
print('----------CELL 08--------')

countries_df = pd.merge(un_country_features, tabular_df, left_on='country_iso_code', right_on='ISO3')
countries_df.drop(columns=['OBJECTID_1','OBJECTID', 'GlobalID', 'Shape__Area','Shape__Length'], inplace=True)
print(countries_df.head())

#---------------------
print('----------CELL 09--------')
if update:
    published_polygon_layer = GeoAccessor.from_df(countries_df, geometry_column='SHAPE')
    polygon_layer = gis.content.get(update_item_polygons)
    existing_features = polygon_layer.layers[0]
    existing_features.manager.truncate()
    existing_features.edit_features(adds=published_polygon_layer)

#---------------------
print('----------CELL 10--------')
if not update:
    print(sdg_key + "_" + stamp)
    published_polygon_layer = countries_df.spatial.to_featurelayer(sdg_key + "_" + stamp)
    
print(published_polygon_layer)

#---------------------
print('----------CELL 11--------')
language_dict = {'english': 'EN', 'spanish': 'ES', 'french': 'FR', 'russian': 'RU', 'chinese': 'ZN'}
lang_str = language_dict[card_lang]

#---------------------
print('----------CELL 12--------')
series_card = dict()
series_desc = series_dict['description'].replace('%', 'percent').replace(',', ' ').replace('/', ' ')
title = 'Indicator ' + indicator_dict['desc'+ lang_str] + ': ' + series_desc
series_card['title'] = (title[:250] + '..') if len(title) > 250 else title
layer_title = 'Indicator_' + \
    indicator_dict['reference'].replace('.', '_') + '__Series_' + series_dict['code']
series_card['layer_title'] = layer_title[:89] if len(layer_title) > 88 else layer_title  
series_card['snippet'] = series_card['title']

series_card['description'] =  \
        '<div style="background-color: #' + goal_dict['hex'] + '; color:#fff; padding: 15px">' + \
        '<ul style="list-style: none;">' + \
        '<li><strong> Series Name:</strong> ' + series_desc + '</li>' + \
        '<li><strong>Series Code:</strong> ' + series_dict['code'] + '</li>' + \
        '<li><strong>Release Version:</strong> ' + series_dict['release'] + '</li>' + \
        '</ul>' + \
        '</div>' + \
        '<div style="background-color: #f4f4f4; padding: 15px">' + \
        '<p> This dataset is the part of the Global SDG Indicator Database compiled ' + \
        'through the UN System in preparation for the Secretary-General\'s annual report on <em>Progress towards the Sustainable Development Goals</em>.' + \
        '</p>' + \
        '<p><strong>Indicator ' + indicator_dict['reference'] + ': </strong>' + indicator_dict['desc'+ lang_str] + \
        '</p>' + \
        '<p><strong>Target ' + target_dict['code'] + ': </strong>' + target_dict['desc'+ lang_str] + \
        '</p>' + \
        '<p><strong>Goal ' + goal_dict['code'] + ': </strong>' + goal_dict['desc'+ lang_str] + \
        '</p>' +  \
        '<p><em>For more information on the compilation methodology of this dataset, ' +\
        ' see <a href="https://unstats.un.org/sdgs/metadata/" target="_blank">https://unstats.un.org/sdgs/metadata/' + \
        '</a></em></p>' + \
        '</div>'

series_tags = series_dict['tags'][:]
series_tags.append(series_dict['release'])
series_card['tags'] = series_tags
display(HTML(series_card['description']))

#---------------------
print('----------CELL 13--------')

point_layer.update(series_card,thumbnail= goal_dict['thumbnail'])
point_layer
#---------------------
print('----------CELL 14--------')

published_polygon_layer.update(series_card,thumbnail= goal_dict['thumbnail'])
published_polygon_layer
#---------------------
print('----------CELL 15--------')


#---------------------
print('----------CELL 16--------')

#Statistic Variable
statistic_field = "latest_value"

#Color Scheme
color_list = goal_dict['ColorScheme']
color_scheme = []

for c in color_list:
    s = str(c)
    rgb_val = tuple(int(s[i:i+2], 16) for i in (0,2,4))
    rgb_list = list(rgb_val)
    rgb_list.append(255)
    color_scheme.append(rgb_val)
#Set Min/Max Values
out_statistics = [{'statisticType': 'max',
                    'onStatisticField': statistic_field,
                    'outStatisticFieldName': statistic_field + '_max'},
                    {'statisticType': 'min',
                    'onStatisticField': statistic_field,
                    'outStatisticFieldName': statistic_field + '_min'}]

polygon_features = published_polygon_layer.layers[0]
print(polygon_features)

print('-------------------------past---------------------------')
feature_set = polygon_features.query(
    where='1=1', out_statistics=out_statistics)

max_value = feature_set.features[0].attributes[statistic_field + '_max']
min_value = feature_set.features[0].attributes[statistic_field + '_min']

value_interval = (max_value - min_value)/10

#---------------------
print('----------CELL 17--------')
drawing_info_json = {"drawingInfo":{
    "renderer":{
        "authoringInfo":{
            "type":"classedColor",
            "classificationMethod":"esriClassifyEqualInterval"
            },
        "type":"classBreaks",
        "field":"latest_value",
        "minValue":min_value,
        "classBreakInfos":[{
            "symbol":{
                "color":color_scheme[0],
                "outline":{
                    "color":[194,194,194,64],
                    "width":0.375,
                    "type":"esriSLS",
                    "style":"esriSLSSolid"
                    },
                "type":"esriSFS",
                "style":"esriSFSSolid"
                },
            "label":str(min_value)+" - "+str(min_value + value_interval),
            "classMaxValue":min_value + value_interval
            },
            {"symbol":{
                "color":color_scheme[1],
                "outline":{
                    "color":[194,194,194,64],
                    "width":0.375,
                    "type":"esriSLS",
                    "style":"esriSLSSolid"
                    },
                "type":"esriSFS",
                "style":"esriSFSSolid"
                },
            "label":"\u003e " + str(min_value + value_interval)+" - "+str(min_value + value_interval*2),
            "classMaxValue":min_value + value_interval*2
            },
            {"symbol":{
                "color":color_scheme[2],
                "outline":{
                    "color":[194,194,194,64],
                    "width":0.375,
                    "type":"esriSLS",
                    "style":"esriSLSSolid"
                    },
                "type":"esriSFS",
                "style":"esriSFSSolid"
                },
            "label":"\u003e " + str(min_value + value_interval*2)+" - "+str(min_value + value_interval*3),
            "classMaxValue":min_value + value_interval*3
            },
            {"symbol":{
                "color":color_scheme[3],
                "outline":{
                    "color":[194,194,194,64],
                    "width":0.375,
                    "type":"esriSLS",
                    "style":"esriSLSSolid"
                    },
                "type":"esriSFS",
                "style":"esriSFSSolid"
                },
            "label":"\u003e " + str(min_value + value_interval*3)+" - "+str(min_value + value_interval*4),
            "classMaxValue":min_value + value_interval*4
            },
            {"symbol":{
                "color":color_scheme[4],
                "outline":{
                    "color":[194,194,194,64],
                    "width":0.375,
                    "type":"esriSLS",
                    "style":"esriSLSSolid"
                    },
                "type":"esriSFS",
                "style":"esriSFSSolid"
                },
            "label":"\u003e " + str(min_value + value_interval*4)+" - "+str(min_value + value_interval*5),
            "classMaxValue":min_value + value_interval*5
            },
            {"symbol":{
                "color":color_scheme[5],
                "outline":{
                    "color":[194,194,194,64],
                    "width":0.375,
                    "type":"esriSLS",
                    "style":"esriSLSSolid"
                    },
                "type":"esriSFS",
                "style":"esriSFSSolid"
                },
            "label":"\u003e " + str(min_value + value_interval*5)+" - "+str(min_value + value_interval*6),
            "classMaxValue":min_value + value_interval*6
            },
            {"symbol":{
                "color":color_scheme[6],
                "outline":{
                    "color":[194,194,194,64],
                    "width":0.375,
                    "type":"esriSLS","style":"esriSLSSolid"
                    },
                "type":"esriSFS",
                "style":"esriSFSSolid"
                },
            "label":"\u003e " + str(min_value + value_interval*6)+" - "+str(min_value + value_interval*7),
            "classMaxValue":min_value + value_interval*7
            },
            {"symbol":{
                "color":color_scheme[7],
                "outline":{
                    "color":[194,194,194,64],
                    "width":0.375,
                    "type":"esriSLS",
                    "style":"esriSLSSolid"
                    },
                "type":"esriSFS",
                "style":"esriSFSSolid"
                },
            "label":"\u003e " + str(min_value + value_interval*7)+" - "+str(min_value + value_interval*8),
            "classMaxValue":max_value
            }],
        "classificationMethod":"esriClassifyEqualInterval"
        },
    "transparency":20}}


#---------------------
print('----------CELL 18--------')
polygon_features.manager.update_definition(drawing_info_json)
print(polygon_features)

#---------------------
print('----------CELL 19--------')
usa_map = gis.map('World', zoomlevel=2)
usa_map.add_layer(point_layer)
usa_map.add_layer(published_polygon_layer)
print(usa_map)