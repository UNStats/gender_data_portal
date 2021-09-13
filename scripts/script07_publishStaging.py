import utils
import os
import utils_arcgis_gender
from bs4 import BeautifulSoup
import json 
import copy

from arcgis.gis import GIS

# ====================================
# Upload to ArcGIS into staging folder
# ====================================

release = '2021.01'

# Layer info template
layer_info = utils.open_json('utilities/layerinfo.json')
# print(layer_info)

layer_info_properties = list(layer_info.keys())

# minset_catalog
minset_catalog = utils.open_json('master_data/minset_catalog.json')

main_fields = utils.select_dict(
    utils.tsv2dictlist('master_data/ts_catalog_edited.csv'), 
    {'main_statistics_field': '1'}
)
# print(main_fields[0])

column_aliases = utils.open_json('master_data/column_aliases_edited.json')

# print(column_aliases[0])


# ----------------------------------------
# Setup global information and variables
# ----------------------------------------

# ArcGIS group with which the data will be shared
global open_data_group
# Variable to keep track of any csv file that cannot be staged
global failed_series
# ArcGIS credentials
global online_username
# ArcGIS connection
global gis_online_connection
# Information pertaining to the layer template
global layer_json_data
# Collection of items owned by user
global user_items

# ----------------------------------------
# User parameters
# ----------------------------------------

property_update_only = False
update_symbology = True
update_sharing = True

# -----------------------------------------
# Set path to data
# -----------------------------------------

data_dir = 'series_data_rearranged'
data_files = os.listdir('series_data_rearranged')

failed_series = []

# Establish ArcGIS connection
online_username, gis_online_connection = utils_arcgis_gender.connect_to_arcGIS()
print(gis_online_connection)

# Get open data group - used for staging personal content before publication into Open Data Site
open_data_group = utils_arcgis_gender.open_data_group(
    gis_online_connection, '1836cf4896d6443d83d1168f057122bf')  # Luis
# open_data_group = open_data_group(
#     gis_online_connection, '967dbf64d680450eaf424ac4a38799ad')  # Travis

print(gis_online_connection)


# ==============================


minsetTree = minset_catalog.copy()  # Produces a shallow copy of series_metadata
# print(minsetTree[0].keys())

for ddx, d in enumerate(minsetTree):

    # if ddx != 0:
    #     continue

    # if d['DOMAIN_ID'] not in ['1']:
    #     continue

    for idx, i in enumerate(d['INDICATORS']):

        # if idx != 0:
        #     continue

        # if i['INDICATOR_ID'] not in ['I98ba1fd8']:
        #     continue

        if 'SERIES' in i.keys():

            for sdx, s in enumerate(i['SERIES']):

                # if sdx != 0:
                #     continue

                # if s['SERIES_ID'] != 'S2d8b1bf8':
                #     continue

                print('\nProcessing series code: (',
                        s['SORT_ORDER'] +') ' + s['SERIES_ID'])

                this_d = {k: d[k]
                            for k in d.keys() if k not in ['INDICATORS']}
                print(f"{this_d=}")

                this_i = {k: i[k]
                            for k in i.keys() if k not in ['SERIES']}
                print(f"{this_i=}")

                s_card = utils_arcgis_gender.build_series_card(
                    this_d, this_i, s, release)

                # print(f"{s_card=}")

                # soup = BeautifulSoup(s_card['description'], 'html.parser')
                # prettyHTML=soup.prettify()   #prettify the html
                # print(prettyHTML)  

                for ca in column_aliases:
                    if ca['file'] != s['SORT_ORDER'] + '__' + i['INDICATOR_LABEL'].replace('.','_') + '__' + i['INDICATOR_ID'] + '__' + s['SERIES_ID'] + '.csv':
                        continue
                    aliases = ca

                for mf in main_fields:
                    if mf['MINSET_SERIES'] != s['SERIES_ID']:
                        continue
                    main_field = mf['TSK_sub_id']

                with open('test/aliases.json', 'w') as fout:
                    json.dump(aliases, fout,  indent=4)  

                with open('test/main_field.json', 'w') as fout:
                    json.dump(main_field, fout,  indent=4)  

                if 'OBS_VALUE' in aliases.keys():
                    main_field = 'OBS_VALUE'
                    print(f"{main_field=}")
                elif 'VALUE_CATEGORY_DESC' in aliases.keys():
                    main_field = 'VALUE_CATEGORY_DESC'
                    print(f"{main_field=}")
                elif 'OBS_VALUE__' + main_field in aliases.keys():
                    main_field = 'OBS_VALUE__' + main_field
                    print(f"{main_field=}")
                elif 'VALUE_CATEGORY_DESC__' + main_field in aliases.keys():
                    main_field = 'VALUE_CATEGORY_DESC__' + main_field
                    print(f"{main_field=}")
                else: 
                    print(f'no main field with {main_field}')

                # Publish dataset to 'Open Data SDGxx' folder in personal account.
                online_item = utils_arcgis_gender.publish_csv(
                    this_d,
                    this_i,
                    s,
                    release,  aliases,
                    item_properties=s_card,
                    thumbnail='https://raw.githubusercontent.com/UNStats/gender_data_portal/main/utilities/ww2020_thumbnail.png',
                    layer_info=layer_info,
                    gis_online_connection=gis_online_connection,
                    online_username=online_username,
                    data_dir=data_dir,
                    statistic_field= main_field,
                    property_update_only=False,
                    color=[247, 139, 51]
                )



                # Only set the sharing when updating or publishing
                if online_item is not None:
                    if update_sharing:
                        # Share this content with the open data group
                        online_item.share(everyone=False,
                                            org=True,
                                            groups=open_data_group["id"],
                                            allow_members_to_edit=False)

                    # display(online_item)

                    # open_data_group.update(tags=open_data_group["tags"] + [series["code"]])
                else:
                    failed_series.append(s["SERIES_ID"])

print(f"{failed_series=}")
