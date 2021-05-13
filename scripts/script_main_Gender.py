

# This module allows to make HTTP requests:
import requests

# This module is required to perform file-related I/O operations (eg. file reading/writing)
import io
import copy

# This module is used to create time stamps
import time

# The pandas module provides data analysis and manipulation functionality 
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# This module contains a few utility functions with frequent tasks, like reading and writing files.
import utils_arcgis

import utils

# This is the main arcGIS library for python. 
from arcgis.gis import GIS

# This will be used to see whether files exist
import os.path
from os import path

#--------------------------------------------------------------------
# Establish ArcGIS connection
online_username, gis = utils_arcgis.connect_to_arcGIS()
print(gis)


#--------------------------------------------------------------------

# Country Layer to join Data to (boundary data)
un_country_layer_id = '25c570fd7f074471b98f5108792aaa84'

#--------------------------------------------------------------------

# This is the column in the dataset that contains the variable to be displayed. Statistic Variable
statistic_field = "OBS_VALUE"

#--------------------------------------------------------------------

un_github_data = "https://github.com/UNStats/gender_data_portal"
un_github_raw = un_github_data.replace('github.com', 'raw.githubusercontent.com')
un_github_metadata = un_github_raw +"/main/master_data/metadata_minset.json"
#---------------------
print(un_github_metadata)
metadata = requests.get(un_github_metadata)
series_metadata = metadata.json()
#print(series_metadata[0])

#--------------------------------------------------------------------

# Choose the indicator and series to be published
set_indicator_test = "12"
set_series_test = "72"
#--------------------------------------------------------------------

for i in series_metadata:

    indicator_dict = copy.deepcopy(i)

    set_indicator = indicator_dict['INDICATOR_ID']


    if set_indicator != set_indicator_test:
        continue


    del indicator_dict['series']

    for s in i['series']:
        

        set_series = s['MINSET_SERIES']
        
        series_dict = s

        series_metadata = s['Metadata']

        # if set_series != set_series_test:
        #     continue

        miset_key = "ind_" + str(set_indicator) + "__series_" + str(set_series)

        #create timestamp to allow rerunning
        stamp = str(time.time() * 1000).replace(".","")

        print('Processing SDG Information for', miset_key, stamp)
        #--------------------------------------------------------------------
        un_github_csv = un_github_raw +"/main/series_data/" + miset_key + ".csv"

        #---------------------

        indicator_properties = list(
            indicator_dict.keys())

        print(indicator_properties)

        series_properties = list(
            s.keys())

        print(series_properties)

        #-----------------------------------------------------


        print(un_github_csv)


        r = requests.get(un_github_csv)
        
        try:
            r.raise_for_status()
        except:
            pass
        if r.status_code != 200:
            print ("Website Error: ", un_github_csv, r)
            continue

        tabular = r.content
        tabular_df = pd.read_csv(io.StringIO(tabular.decode('utf-8')), sep='\t')
        print(tabular_df.head(5))

        #----------------------------------------------------

        tabular_df['latitude'] = tabular_df['Y']
        tabular_df['longitude'] = tabular_df['X']
        print(tabular_df.info())

        #-----------------------------------------------------

        stamp = str(time.time() * 1000).replace(".","")
        csv_file = 'series_data_prepared/' + miset_key + "_" + stamp +'.csv'
        #-----------------------------------------------------
            
        tabular_df.to_csv(csv_file, encoding="utf-8", index=False)
        csv_item = gis.content.add({}, csv_file)
        #Analyze the CSV File for Publishing
        analyze_item = gis.content.analyze(item=csv_item,location_type='coordinates')
        publish_parameters=analyze_item['publishParameters']

        point_layer = csv_item.publish()
        #-----------------------------------------------------
        un_country_item = gis.content.get(un_country_layer_id)
        print(un_country_item)

        un_country_layer = un_country_item.layers[0]
        un_country_features = un_country_layer.query(where='1=1', as_df=True)
        un_country_features

        countries_df = pd.merge(un_country_features, tabular_df, left_on='ISO3', right_on='ISO3')
        countries_df.drop(columns=['FID','POP2005', 'REGION', 'LON', 'LAT','Shape__Area','Shape__Length'], inplace=True)
        countries_df.info()

        feature_layer_key = miset_key + "_" + stamp
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
                '  <ul style="list-style: none;">' + \
                '    <li><strong> Series:</strong> ' +  series_dict['MINSET_SERIES_DESC'] + '</li>' + \
                '    <li><strong> Indicator: </strong>' + indicator_dict['INDICATOR_DESC'] + \
                '  </ul>' + \
                '</div>' + \
                '<div style="background-color: #f4f4f4; padding: 15px">' + \
                '  <p> This dataset is the part of the Minimum Set of Gender Indicators compiled ' + \
                '      through the United Nations Statistics Division. </p>'



        for sm in series_metadata:
            if sm['METADATA_CATEGORY'] in ("2", "3", "4", "5", "6", "7"):

                category_desc = sm['METADATA_CATEGORY_DESC']
                metadata_desc = sm['METADATA_DESCRIPTION']

                series_card['description'] = series_card['description']  + \
                    '  <h3>' + utils.none_str_to_empty(category_desc) + '</h3> ' + \
                    '  <p>' +  utils.none_str_to_empty(metadata_desc)  + '</p> ' + \
                    '  <hl>'


        print(series_card['description'])

        series_card['description'] =  series_card['description'] + \
                '  <p><em>For more information on the compilation methodology of this dataset, ' +\
                '     see <a href="https://unstats.un.org/unsd/demographic-social/gender/" target="_blank"> ' +\
                '         Social and Gender Statistics Section, United Nations Statistics Division' + \
                '     </a></em></p>' + \
                '</div>'





        series_card['tags'] = ['Minimum Gender Data Set']

        print(series_card)

        #-----------------

        thumbnail = 'https://raw.githubusercontent.com/UNStats/gender_data_portal/main/thumbnails/ww2020_thumbnail.png'
        point_layer.update(series_card,thumbnail= thumbnail)
        published_polygon_layer.update(series_card,thumbnail= thumbnail)

        #-----------------

        series_title = series_card["title"]
        if len(series_title) > 256:
            series_title = series_title[:250] + '..'
