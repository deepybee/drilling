#!/usr/bin/env python3

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from ssl import create_default_context
import re
import json
import boto3
import argparse
import glob
import logging

def _try_to_float(v):
    try:
        return float(v)
    except Exception as e:
        return v    

parser = argparse.ArgumentParser(description='Processes Wellbore / TOPS data files and indexes them into an Elasticsearch cluster.')

parser.add_argument('--es-url', metavar='elasticsearch.mydomain.com', type=str,
                   help='the Elasticsearch cluster URL', required=True)
parser.add_argument('--es-port', metavar= 9200, type=int, default=9200,
                   help='the port Elasticsearch listens on')
parser.add_argument('--index', metavar='tops_data', type=str,
                   help='the index the documents should be written to', default='tops_data')
parser.add_argument('--insecure', metavar=False, type=bool, default=False,
                   help='set to True (not lowercase true) if connecting to a cluster not secured with SSL/TLS')
parser.add_argument('--user', metavar='elastic', type=str,
                    help='not needed if connecting to an unsecured cluster')
parser.add_argument('--password', metavar='Pa55w0rd', type=str,
                   help='not needed if connecting to  an unsecured cluster',)
parser.add_argument('--ca-cert', metavar='/path/to/ca/ca.crt', type=str,
                   help='not needed if connecting to a cluster not secured with SSL/TLS')
parser.add_argument('--directory', metavar='directory', type=str,
                   help='The file system directory to find all tops files', required=True)


args = parser.parse_args()

if args.insecure:
    full_es_url = 'http://' + args.es_url + ':' + str(args.es_port)
    es = Elasticsearch(full_es_url)

else:
    full_es_url = 'https://' + args.es_url + ':' + str(args.es_port)
    if args.ca_cert is None:
        es = Elasticsearch(full_es_url, http_auth=(args.user, args.password))
    else:
        context = create_default_context(cafile=args.ca_cert)
        es = Elasticsearch(full_es_url, ssl_context=context, http_auth=(args.user, args.password))


index_mapping = {
    "mappings" : {
      "_doc": {
        "properties" : {
          "geo_point" : {
            "type": "geo_point"
          }
        }
      }
    }
  }

if es.indices.exists(index=args.index) is False:
    es.indices.create(index=args.index, body=index_mapping)

for _ in ("boto", "elasticsearch", "urllib3"):
    logging.getLogger(_).setLevel(logging.CRITICAL)

logging.basicConfig(
level=logging.INFO,
format="%(asctime)s [%(levelname)-5.5s]  %(message)s",
handlers=[
    logging.FileHandler("tops.log"),
    logging.StreamHandler()
])

tops_data_files = list(glob.iglob(f"{args.directory}/tops.*", recursive = True))

print('\nFound ' + str(len(tops_data_files)) + ' Wellbore data files')

total_docs = 0
total_files = 0

print('Parsing and indexing data into the', args.index, 'index in the Elasticsearch cluster at', full_es_url, '\n')

def create_dict(key_list, value_list):
    return dict(zip(key_list, value_list))


def parse_tops_data(top_data_doc):
    log = open(top_data_doc, 'r')

    source_input = log.read().replace('C-', '')

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
    for sliced in upper:
        upper[index] = re.sub('\s{2,}', '', sliced)
        upper[index] = upper[index].strip()
        upper[index] = upper[index].replace('---', '')
        index += 1

    index = 0
    for sliced in lower:
        lower[index] = re.sub('\s{2,}', '', sliced)
        index += 1

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

    upper_dictionary = create_dict(column_names, values)

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
        lower_dictionary = {}
        lower_dictionary = create_dict(lower_headers, split)
        dict_for_frame = {
            "meta": {k: _try_to_float(v) for k,v in upper_dictionary.items() if v is not ""},
            "data": {k: _try_to_float(v) for k,v in lower_dictionary.items() if v is not ""},
            "geo_point": {
                "lat": float(upper_dictionary['bhl_lat']),
                "lon": float(upper_dictionary['bhl_lon'])
            }
        }
        docs_list.append(dict_for_frame)

    index = 0
    json_docs = []

    for doc in docs_list:
        json_docs.append(
            {
              "_index": args.index,
              "_type": "_doc",
              "_source": doc
            }
        )
            
    # Remove errant first index which contained garbage escaped data
    json_docs.pop(0)

    body = [
        {'index': {'_index': args.index, '_type': '_doc'}}
    ]

    body.extend(json_docs)

    return json_docs

if __name__ == "__main__":
    all_data = []
    for i, doc in enumerate(tops_data_files):
        logging.info(f"{i}/{len(tops_data_files)} {doc}")
        all_data.extend(parse_tops_data(doc))
    logging.info(f"Uploading {len(all_data)} documents to ES")

    helpers.bulk(es, all_data, raise_on_exception=False, raise_on_error=False)



