#!/usr/bin/env python3

from elasticsearch import Elasticsearch
from ssl import create_default_context
import re
import json

context = create_default_context(cafile="/path/to/ca.crt")
es = Elasticsearch("https://localhost:9200", ssl_context=context, http_auth=('elastic', 'Pa55w0rd'))

log = open('topsdata.log', 'r')

source_input = log.read().replace('C-', '')

log.close()

source_input = source_input.replace('\n', '')
source_input = source_input.replace('TVD', '|tvd', 1)
source_input = source_input.replace('SPUD', '|spud', 1)
source_input = source_input.replace('COMP', '|comp', 1)
source_input = source_input.replace('LAT', '|sur_lat', 1)
source_input = source_input.replace('LAT', '|bhl_lat', 1)
source_input = source_input.replace('X=', '|sur_x=', 1)
source_input = source_input.replace('Y=', '|sur_y=', 1)
source_input = source_input.replace('X=', '|bhl_x=', 1)
source_input = source_input.replace('Y=', '|bhl_y=', 1)
source_input = source_input.replace('%', '|')
source_input = source_input.replace('#', '|')
source_input = source_input.replace('SEARCHWELLSURVEYS      TOPS-VERSION', 'SEARCHWELLSURVEYS TOPS-VERSION')
source_input = source_input.replace('PS=', '|PS=', 1)
source_input = source_input.replace('=', ':')
source_input = source_input.replace('UNIQUE_WELLNAME-:', '|UNIQUE_WELLNAME:')
source_input = source_input.replace('MINIMUM CURVATURE     SPHEROID', 'MINIMUM CURVATURE SPHEROID')

for non_delimited in 'WD', 'STATUS', 'PWI':
    source_input = source_input.replace(non_delimited, '|' + non_delimited)

to_split = (source_input.split('|'))

upper = to_split[8:61]
lower = to_split[264:]

upper[2] = 'date: ' + upper[2]
upper[11] = 'time: ' + upper[11]

index = 0
for slice in upper:
    upper[index] = re.sub('\s{2,}', '', slice)
    upper[index] = upper[index].strip()
    upper[index] = upper[index].replace('---', '')
    index += 1

index = 0
for slice in lower:
    lower[index] = re.sub('\s{2,}', '', slice)
    index += 1
# lower_values(upper)
upper[11] = upper[11].replace(':', '-')
upper[11] = upper[11].replace('-', ':', 1)
upper[15] = upper[15].replace(':', '-')
upper[15] = upper[15].replace('-', ':', 1)

column_names = upper[:]
values = upper[:]

header_strip = re.compile('^.+:')
value_strip = re.compile(':.+$')

index = 0
empty_headers = []

for field in column_names:
    if index != [11]:
        column_names[index] = re.sub(value_strip, '', field)
        column_names[index] = column_names[index].replace(':', '')
    column_names[index] = column_names[index].replace(' ', '_')
    column_names[index] = column_names[index].lower()
    if column_names[index] == '':
        empty_headers.append(index)
    index += 1

for field in empty_headers:
    column_names.remove('')

# Fix misaligned indexes due to slicing and dicing
values.pop(9)
values.pop(9)
values.pop(15)
values.pop(15)
values.pop(26)
values.pop(26)
values.pop(34)
values.pop(34)

index = 0
for field in values:
    values[index] = re.sub(header_strip, '', field)
    values[index] = values[index].strip()
    index += 1

upper_dictionary = dict(zip(column_names, values))

lower_headers = ['marker_top-md', 'marker_base-md', 'marker_name_sand-zone_name', 'mrk_cd', 'marker_comments', 'dip_ang', 'dip_azm', 'observed_net_gas', 'observed_net_oil', 'observed_net_sand', 'por_percent', 'sw_percent', 'perm_mdarc',
                 'tempr_deg', 'pressure_gradient_fluid', 'pressure_gradient_mud', 'pressure_gradient_leak', 'fault_plus_or_minus', 'age_mm-yrs', 'mrk-top_subsea', 'mrk-base_subsea', 'corrected_net-v_gas', 'corrected_net-v_oil', 'corrected_net-v_sand',
                 'corr_factor-tvt', 'corr_factor-tst', 'hole_angl', 'hole_azmth', 'dip_interp-ang', 'dip_interp-dir', 'proj_syst', 'grid_coordinates_x', 'grid_coordinates_y', 'subdatum_z-depth', 'seismic_time', 'from_surf_east-west',
                 'from_surf_north-south', 'closure','longitude-latitude-water_depth-tvd_depth', 'data_source', 'well_data', 'comments', 'type', 'obs_num', 'size', 'comments2']

def split_lower(list_of_lower_values, index_to_split_on):
    for field_index in range(0, len(list_of_lower_values), index_to_split_on):
        yield list_of_lower_values[field_index:field_index + index_to_split_on]

lower_values = list(split_lower(lower, 45))

docs_list = []

for split in lower_values:
    lower_dictionary = dict(zip(lower_headers, split))
    print(lower_dictionary)
    dict_for_frame = {**upper_dictionary, **lower_dictionary}
    docs_list.append(dict_for_frame)

index = 0

for dict in docs_list:
    docs_list[index] = json.dumps(docs_list[index])

def dict2doc(current_dict):
    body = []
    body.append({'index': {'_index': 'tops_data', '_type': '_doc'}})
    body.append(current_dict)

    response = es.bulk(body=body)

if __name__ == "__main__":
    for doc in docs_list:
        dict2doc(doc)