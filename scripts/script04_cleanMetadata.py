import utils
import html2markdown


metadata = utils.open_json(
    'master_data/metadata_minset.json')

metadata_review = []

for i in metadata:
    # if i['INDICATOR_ID'] not in ['1']:
    #     continue

    if 'series' in i.keys():
        for s in i['series']:

            # if s['MINSET_SERIES'] != '72':
            #     continue

            print('\nProcessing series code:',
                    i['INDICATOR_ID'], s['MINSET_SERIES'])

            #----------------------------------------------------       

            # Reference the metadata of the current indicator:
            
            if 'Metadata' in s.keys():
                for m in s['Metadata']:
                    metadata_item_md = dict()
                    if 'METADATA_DESCRIPTION' in m.keys():
                        if m['METADATA_DESCRIPTION']:
                           m['METADATA_DESCRIPTION'] = html2markdown.convert(m['METADATA_DESCRIPTION'])
                    
                    metadata_item_md['INDICATOR_ID'] = i['INDICATOR_ID']
                    metadata_item_md['MINSET_SERIES'] = s['MINSET_SERIES']
                    metadata_item_md['METADATA_CATEGORY'] = m['METADATA_CATEGORY']
                    metadata_item_md['METADATA_CATEGORY_DESC'] = m['METADATA_CATEGORY_DESC']
                    metadata_item_md['METADATA_DESCRIPTION'] = m['METADATA_DESCRIPTION'] 
                    metadata_item_md['METADATA_SOURCE'] = m['METADATA_SOURCE']    

                    metadata_review.append(metadata_item_md) 

utils.dictList2tsv(metadata_review, 'master_data/metadata_review.txt')               
                    

            
