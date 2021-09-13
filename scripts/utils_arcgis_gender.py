from arcgis.gis import GIS
import sys
import json
import getpass
import os
import utils
import copy
import requests


def connect_to_arcGIS():
    """Open connection to ArcGIS Online Organization"""

    online_username = input('Username: ')
    online_password = getpass.getpass('Password: ')
    online_connection = "https://www.arcgis.com"
    gis_online_connection = GIS(online_connection,
                                online_username,
                                online_password)

    return online_username, gis_online_connection


def open_data_group(gis_online_connection, id):
    """Explore existing open data gruop"""

    open_data_group = gis_online_connection.groups.get(id)
    return (open_data_group)


def cleanup_staging_folder(user_items):
    """ Cleanup staging folder for Open Data (delete everything in the staging folder for Open Data)"""

    if input('Do you want to cleanup your staging folder for Open Data? (y/n)') == 'y':
        if input('Are you sure? (y/n)') == 'y':
            for item in user_items:
                print('deleting item ' + item.title)
                item.delete()
        else:
            print('Cleanup of staging forlder for Open Data was canceled')
    else:
        print('Cleanup of staging forlder for Open Data was canceled')


def build_series_card(d, i, s, release):
    """ Build series metadata card """

    try:
        s_card = dict()

        s_desc = s['SERIES_DESC'].replace('%', 'percent').replace(
            ',', ' ').replace('/', ' ')

        title = s_desc

        s_card['title'] = (title[:250] + '..') if len(title) > 250 else title

        layer_title = i['INDICATOR_ID'] + '__' + s['SERIES_ID'] 

        s_card['layer_title'] = layer_title[:89] if len(
            layer_title) > 88 else layer_title  # this is very important!!!

        s_card['snippet'] = s_card['title']


        #s_card['snippet'] = (snippet[:250] + '..') if len(snippet) > 250 else snippet
        s_card['description'] =  \
            '<div style="background-color: #f78b33; color:#fff; padding: 15px">' + \
            '<ul style="list-style: none;">' + \
            '<li><strong> Series Name:</strong> ' + s_desc + '</li>' + \
            '<li><strong>Series Code:</strong> ' + s['SERIES_ID'] + '</li>' + \
            '<li><strong>Release Version:</strong> ' + release + '</li>' + \
            '</ul>' + \
            '</div>' + \
            '<div style="background-color: #f4f4f4; padding: 15px">'+ \
            '<p> This dataset is part of the Minimum Gender Dataset compiled by the United Nations Statistics Division.' + \
            '</p>'+ \
            '<p><strong>Indicator ' + i['INDICATOR_LABEL'] + ': </strong>' + i['INDICATOR_DESC'] + \
            '</p>'+ \
            '<p><strong>Domain ' + d['DOMAIN_ID'] + ': </strong>' + d['DOMAIN_DESC'] + \
            '</p>' + \
            '</div>'

        series_tags = s['TAGS'].split(', ')
            

     
        series_tags.append(release)

        s_card['TAGS'] = series_tags

        return s_card
    except:
        print('Unexpected error:', sys.exc_info()[0])
        return None


def find_online_item(title, owner, gis_online_connection, force_find=True):

    try:

        # Search for this ArcGIS Online Item
        query_string = "title:'{}' AND owner:{}".format(title, owner)
        print('Searching for ' + title)
        # The search() method returns a list of Item objects that match the
        # search criteria
        search_results = gis_online_connection.content.search(query_string)

        if search_results:
            for item in search_results:
                if item['title'] == title:
                    print(' -- Item ' + title + ' found (simple find)')
                    return item

        if force_find:
            user = gis_online_connection.users.get(owner)
            user_items = user.items(folder='Open Data', max_items=800)
            for item in user_items:
                if item['title'] == title:
                    print(' -- Item ' + title + ' found (force find)')
                    return item
            print(' -- Item ' + title + ' not found (force find)')
            return None

        print(' -- Item ' + title + ' not found (simple find)')
        return None

    except:
        print('Unexpected error:', sys.exc_info()[0])
        return None


def generate_renderer_infomation(feature_item,
                                 statistic_field,
                                 layer_info,
                                 color=None):
    try:
        if len(color) == 3:
            color.append(130)  # ---specifies the alpha channel of the color

        visual_params = layer_info['layerInfo']
        definition_item = feature_item.layers[0]

        # get the min/max values
        out_statistics = [{'statisticType': 'max',
                           'onStatisticField': statistic_field,
                           'outStatisticFieldName': statistic_field + '_max'},
                          {'statisticType': 'min',
                           'onStatisticField': statistic_field,
                           'outStatisticFieldName': statistic_field + '_min'}]
        # This fails:
        feature_set = definition_item.query(
            where='1=1', out_statistics=out_statistics)

        max_value = feature_set.features[0].attributes[statistic_field + '_max']
        min_value = feature_set.features[0].attributes[statistic_field + '_min']

        visual_params['drawingInfo']['renderer']['visualVariables'][0]['minDataValue'] = min_value
        visual_params['drawingInfo']['renderer']['visualVariables'][0]['maxDataValue'] = max_value

        visual_params['drawingInfo']['renderer']['authoringInfo']['visualVariables'][0]['minSliderValue'] = min_value
        visual_params['drawingInfo']['renderer']['authoringInfo']['visualVariables'][0]['maxSliderValue'] = max_value

        visual_params['drawingInfo']['renderer']['classBreakInfos'][0]['symbol']['color'] = color
        visual_params['drawingInfo']['renderer']['transparency'] = 25

        definition_update_params = definition_item.properties
        definition_update_params['drawingInfo']['renderer'] = visual_params['drawingInfo']['renderer']
        if 'editingInfo' in definition_update_params:
            del definition_update_params['editingInfo']
        definition_update_params['capabilities'] = 'Query, Extract, Sync'
        print('Update Feature Service Symbology')
        definition_item.manager.update_definition(definition_update_params)

        return
    except:
        print('Unexpected error in generate_renderer_infomation:',
              sys.exc_info()[0])
        return None


def publish_csv(d, i, s, release, aliases,
                item_properties,
                thumbnail,
                layer_info,
                gis_online_connection,
                online_username,
                data_dir,
                statistic_field='latest_value',
                property_update_only=False,
                color=[169, 169, 169]):

    # Check if service name is available; if not, update the link
    service_title = s['SERIES_ID'] + '_' + \
        i['INDICATOR_ID'].replace('.', '_') + '_' + release.replace('.', '')

    # display(service_title)

    service_title_num = 1

    while not gis_online_connection.content.is_service_name_available(service_name=service_title,
                                                                      service_type='featureService'):
        service_title = i['INDICATOR_ID'].replace('.', '_') + '_' + s['SERIES_ID'] + '_' + release.replace('.', '') + \
            '_' + str(service_title_num)
        service_title_num += 1

    # display(service_title_num)

    # csv file to be uploaded:
    file = os.path.join(data_dir, s['SORT_ORDER'] + '__' + i['INDICATOR_LABEL'].replace('.', '_') + '__' + i['INDICATOR_ID'] + '__' + s['SERIES_ID'] + '.csv')

    print(file)

    if os.path.isfile(file):
        csv_item_properties = copy.deepcopy(item_properties)
        csv_item_properties['name'] = service_title
        csv_item_properties['title'] = service_title
        csv_item_properties['type'] = 'CSV'
        csv_item_properties['url'] = ''

        # display(csv_item_properties)

        # Does this CSV already exist
        csv_item = find_online_item(
            csv_item_properties['title'], online_username, gis_online_connection)

        if csv_item is None:
            print('Adding CSV File to ArcGIS Online....')

            # display(gis_online_connection)
            # display(thumbnail)

            csv_item = gis_online_connection.content.add(item_properties=csv_item_properties,
                                                         thumbnail=thumbnail,
                                                         data=file)

            if csv_item is None:
                return None

            print('Analyze Feature Service....')

            # Change attribute types:
            publish_parameters = analyze_csv(
                csv_item['id'], gis_online_connection, aliases)

            if publish_parameters is None:
                return None
            else:
                publish_parameters['name'] = csv_item_properties['title']
                publish_parameters['layerInfo']['name'] = csv_item_properties['layer_title']

                print('Publishing Feature Service....')
                csv_lyr = csv_item.publish(
                    publish_parameters=publish_parameters, overwrite=True)

                # Update the layer infomation with a basic rendering based on the Latest Value
                # use the hex color from the SDG Metadata for the symbol color

                print('.......call generate renderer within publish_csv')
                generate_renderer_infomation(feature_item=csv_lyr,
                                             statistic_field=statistic_field,
                                             layer_info=layer_info,
                                             color=color)

        else:
            # Update the Data file for the CSV File
            csv_item.update(item_properties=csv_item_properties,
                            thumbnail=thumbnail, data=file)

            # Find the Feature Service and update the properties
            csv_lyr = find_online_item(
                csv_item_properties['title'], online_username, gis_online_connection)

        # Move to the Open Data Folder
        if csv_item['ownerFolder'] is None:
            print('Moving CSV to Open Data Folder')
            csv_item.move('Minimum Gender Dataset - ' + d['DOMAIN_ID'])

        if csv_lyr is not None:
            print('Updating Feature Service metadata....')
            csv_lyr.update(item_properties=item_properties,
                           thumbnail=thumbnail)

            if csv_lyr['ownerFolder'] is None:
                print('Moving Feature Service to Open Data Folder')
                csv_lyr.move('Minimum Gender Dataset - ' + d['DOMAIN_ID'])

            return csv_lyr
        else:
            return None
    else:
        print('File ' + file + ' does not exist.')
        return None


def analyze_csv(item_id, gis_online_connection, aliases):
    try:
        sharing_url = gis_online_connection._url + \
            '/sharing/rest/content/features/analyze'

        analyze_params = {'f': 'json',
                          'token': gis_online_connection._con.token,
                          'sourceLocale': 'en-us',
                          'filetype': 'csv',
                          'itemid': item_id}

        r = requests.post(sharing_url, data=analyze_params)

        analyze_json_data = json.loads(r.content.decode('UTF-8'))

        for field in analyze_json_data['publishParameters']['layerInfo']['fields']:
            for k,v in aliases.items():
                if k != field['name']:
                    continue
                else:
                    field['alias'] = v

            # display(field['name'])
            # display(field['type'])
            # display(field['sqlType'])
            # print('---')

            if field['name'] == 'TIME_PERIOD':
                field['type'] = 'esriFieldTypeInteger'
                field['sqlType'] = 'sqlTypeInt'

            elif field['name'].startswith('OBS_VALUE'):
                field['type'] = 'esriFieldTypeDouble'
                field['sqlType'] = 'sqlTypeFloat'

            else:
                field['type'] = 'esriFieldTypeString'
                field['sqlType'] = 'sqlTypeNVarchar'

        # set up some of the layer information for display
        analyze_json_data['publishParameters']['layerInfo']['displayField'] = 'latest_value'
        return analyze_json_data['publishParameters']
    except:
        print('Unexpected error:', sys.exc_info()[0])
        return None


def set_field_alias(field_name):

    if field_name == 'goal_code':
        return 'Goal Code'
    elif field_name == 'goal_labelEN':
        return 'Goal Label'
    elif field_name == 'goal_descEN':
        return 'Goal Description '
    elif field_name == 'target_code':
        return 'Target Code'
    elif field_name == 'target_descEN':
        return 'Target Description'
    elif field_name == 'indicator_code':
        return 'Indicator Code'
    elif field_name == 'indicator_reference':
        return 'Indicator Reference'
    elif field_name == 'indicator_tier':
        return 'Tier'
    elif field_name == 'indicator_custodians':
        return 'Custodian Agency'
    elif field_name == 'indicator_descEN':
        return 'Indicator Description'
    elif field_name == 'series_release':
        return 'Series Release'
    elif field_name == 'series_tags':
        return 'Tags'
    elif field_name == 'series':
        return 'Series Code'
    elif field_name == 'seriesDescription':
        return 'Series Description'
    elif field_name == 'geoAreaCode':
        return 'Geographic Area Code'
    elif field_name == 'geoAreaName':
        return 'Geographic Area Name'
    elif field_name == 'level':
        return 'Geographic Area Level'
    elif field_name == 'parentCode':
        return 'Parent Geographic Area Code'
    elif field_name == 'parentName':
        return 'Parent Geographic Area Name'
    elif field_name == 'type':
        return 'Geographic Area Type'
    elif field_name == 'ISO3':
        return 'ISO Code'
    elif field_name == 'UN_Member':
        return 'Is UN Member'
    elif field_name == 'Country_Profile':
        return 'Has Country Proile'
    elif field_name == 'years':
        return 'Available Years'
    elif field_name == 'min_year':
        return 'Earliest Year Available'
    elif field_name == 'max_year':
        return 'Latest Year Available'
    elif field_name == 'n_years':
        return 'Number of Years Available'
    elif field_name == 'unitsCode':
        return 'Measurement Unit Code'
    elif field_name == 'unitsDesc':
        return 'Measurement Unit Description'
    elif field_name == 'reportingTypeCode':
        return 'Reporting Type Code'
    elif field_name == 'reportingTypeDesc':
        return 'Reporting Type Description'
    elif field_name == 'basePeriod':
        return 'Base Period'
    elif field_name == 'valueDetails':
        return 'Value Details'
    elif field_name == 'footnotes':
        return 'Footnotes'
    elif field_name == 'sources':
        return 'Sources'
    elif field_name == 'timeDetails':
        return 'Time Period Details'
    elif field_name == 'nature':
        return 'Nature'
    else:
        return utils.camel_case_split(field_name.replace('_', ' ')).replace(' Desc', ' Description').title()


# def update_item_categories(item, goal, target, gis_online_connection):
#     update_url = gis_online_connection._url + "/sharing/rest/content/updateItems"
#     items = [{item["id"]:{"categories": [
#         "/Categories/Goal " + str(goal) + "/Target " + str(target)]}}]
#     update_params = {'f': 'json',
#                      'token': gis_online_connection._con.token,
#                      'items': json.dumps(items)}
#     r = requests.post(update_url, data=update_params)
#     update_json_data = json.loads(r.content.decode("UTF-8"))
#     print(update_json_data)


def set_content_status(gis_online_connection, update_item, authoratative=True):
    sharing_url = gis_online_connection._url + \
        "/sharing/rest/content/items/" + update_item.id + "/setContentStatus"
    sharing_params = {'f': 'json', 'token': gis_online_connection._con.token,
                      'status': 'org_authoritative' if authoratative else 'deprecated',
                      'clearEmptyFields': 'false'}
    r = requests.post(sharing_url, data=sharing_params)
    sharing_json_data = json.loads(r.content.decode("UTF-8"))
