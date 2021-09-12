import utils
import os
from bs4 import BeautifulSoup
import json 


# Read minset_indicators_catalog
minset_indicators_catalog = utils.xlsx2dict('master_data/CL_INDICATORS.xlsx', 0)
# with open('temp/minset_indicators_catalog.json', 'w') as fout:
#     json.dump(minset_indicators_catalog, fout,  indent=4)

domains_catalog = utils.unique_dicts(
    utils.subdict_list(minset_indicators_catalog, ['DomainID', 'DomainName'])
    )
# with open('temp/domains_catalog.json', 'w') as fout:
#     json.dump(domains_catalog, fout,  indent=4)


minset_series_catalog = utils.xlsx2dict('master_data/CL_SERIES.xlsx', 0)
# with open('temp/minset_series_catalog.json', 'w') as fout:
#     json.dump(minset_series_catalog, fout,  indent=4)

minset_catalog = []

for ddx, domain in enumerate(domains_catalog):
    
    # if ddx != 0:
    #     continue

    d = dict()
    d['DOMAIN_ID'] = domain['DomainID']
    d['DOMAIN_DESC'] = domain['DomainName']
    d['INDICATORS'] = []


    for idx, indicator in enumerate(
        utils.select_dict(
            minset_indicators_catalog, {'DomainID': d['DOMAIN_ID']}
        )
    ):

        # if idx != 0:
        #     continue

        if indicator['Retired'] == '1':
            continue

        d_ind = dict()
        d_ind['INDICATOR_ID'] = 'I'+indicator['INDICATOR_ID']
        d_ind['INDICATOR_LABEL'] = indicator['INDICATOR_LABEL']
        d_ind['INDICATOR_DESC'] = indicator['INDICATOR_DESC']
        d_ind['QUALITATIVE'] = indicator['Qualitative']
        d_ind['TIER'] = indicator['Tier']
        d_ind['EDGE_ID'] = indicator['EdgeID']
        d_ind['EDGE_DESC'] = indicator['EdgeName']
        d_ind['BPFA'] = indicator['BPFA']
        d_ind['SDG'] = indicator['SDG']
        d_ind['SDG_IDICATOR'] = indicator['SDG_IndicatorID']
        d_ind['MDG'] = indicator['MDG']
        d_ind['AGENCY_ID'] = indicator['AgencyID']
        d_ind['Agencies_DESC'] = indicator['AgenciesLabel']
        d_ind['COMMENT'] = indicator['Comment']
        d_ind['INDICATOR_ID_pre2021'] = indicator['INDICATOR_ID_pre2021']
        d_ind['INDICATOR_NUM_pre2021'] = indicator['INDICATOR_NUM_pre2021']

        d_ind['SERIES'] = []

        for sdx, series in enumerate(
            utils.select_dict(
                minset_series_catalog, {'INDICATOR_ID': d_ind['INDICATOR_ID']}
            )
        ):

            
            # if sdx != 0:
            #     continue

            if series['Retired'] == '1':
                continue

            d_series = dict()

            d_series['SORT_ORDER'] = series['SORT_ORDER']
            d_series['SERIES_ID'] = series['MINSET_SERIES']
            d_series['SERIES_DESC'] = series['MINSET_SERIES_DESC']
            d_series['SDG_SERIES_ID'] = series['SDG_SeriesID']
            d_series['ILO_SERIES_ID'] = series['ILO_SeriesID']
            d_series['UNESCO_SERIES_ID'] = series['UNESCO_SeriesID']
            d_series['TAGS'] = series['Tags']
            d_series['COMMENTS'] = series['Comment']
            d_series['SERIES_ID_pre2021'] = series['MINSET_SERIES_pre2021']

            d_ind['SERIES'].append(d_series)

        d['INDICATORS'].append(d_ind)
    
    minset_catalog.append(d) 


with open('master_data/minset_catalog.json', 'w') as fout:
    json.dump(minset_catalog, fout,  indent=4)     


