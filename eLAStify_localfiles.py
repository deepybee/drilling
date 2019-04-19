#!/usr/bin/env python3

import pandas as pd
from LAS import Converter
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from ssl import create_default_context
import re
import argparse
import boto3
import glob
import logging

def _is_float(v):
  try:
    float(v)
    return True
  except Exception as e:
    return False

def parse_las_data(las_data_doc, index_name, es):
    c = Converter()

    # Read it. If something goes wrong, skip the file
    log = None
    try:
      log = c.set_file(las_data_doc)
    except Exception as ex:
      logging.warn(ex)
      return False

    meta_data = {}
    curve_data = {}
    data = {}

    # Programatically get all meta data fields and store them nicely
    for meta_key, meta_value in log.get_dict()['well'].items():
      if meta_value is not None:
        meta_data[meta_value['desc'].replace(' ', '_').lower()] = meta_value['value']

    # If the expected latitude and longitude fields are not present - skip
    if 'surf._latitude' not in meta_data or 'surf._longitude' not in meta_data:
      logging.warn('Different latitude and longitude fields present.. skipping')  

      return False

    # If the latitude and longitude formats are not in the expected format - skip
    if _is_float(meta_data['surf._latitude']) is False or _is_float(meta_data['surf._longitude']) is False:
      logging.warn("Different latitude or longitude format. Only supporting decimal format as that is what was provided in the sample... Skipping")
      return False

    # Programatically get all curve data and names and store them nicely
    for curve_key, curve_value in log.get_dict()['curve'].items():
      curve_data[curve_key.lower()] = {
        "name": curve_value['desc'].split("  ")[1].replace(' ', '_').lower(),
        "unit": curve_value['unit']
      }
      # Get the actual curve data and store it in a dict for Pandas to read
      data[curve_key.lower()] = log.get_dict()['data'][curve_key.lower()]

    # Read the curve data into pandas which automagically tidies a lot up
    try:
      las_df = pd.DataFrame(data)
    except Exception as ex:
      logging.error(ex)
      return False

    all_data = []

    #Iterate over every row in the data
    for _, row in las_df.iterrows():
      # Get each row as json and remove any fields with the null value in them
      clean_row = {curve_data[key]['name']:val for key, val in row.items() if val != -999.2500}

      # Build up the Elasticsearch document
      all_data.append(
        {
          "_index": index_name,
          "_type": "_doc",
          "_source": {
            "data": clean_row,
            "geo_point": {
              "lat": meta_data['surf._latitude'],
              "lon": meta_data['surf._longitude']
            },
            **meta_data
          }
        }
      )

    # Upload the entire LAS file
    logging.info(f"Uploading {len(all_data)}")
    helpers.bulk(es, all_data, raise_on_exception=False, raise_on_error=False)

    logging.info(f'Indexed {len(all_data)} records from LAS data file {str(las_data_doc)}')
    return True

if __name__ == "__main__":
  for _ in ("boto", "elasticsearch", "urllib3"):
    logging.getLogger(_).setLevel(logging.CRITICAL)

  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.FileHandler("las.log"),
        logging.StreamHandler()
    ])


  parser = argparse.ArgumentParser(description='Processes LAS data files and indexes them into an Elasticsearch cluster.')

  parser.add_argument('--es-url', metavar='elasticsearch.mydomain.com', type=str,
                     help='the Elasticsearch cluster URL', required=True)
  parser.add_argument('--es-port', metavar= 9200, type=int, default=9200,
                     help='the port Elasticsearch listens on')
  parser.add_argument('--index', metavar='las_data', type=str,
                     help='the Elasticsearch cluster URL', default='las_data')
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


  if es.indices.exists(index=args.index) is False:
    es.indices.create(index=args.index, body=index_mapping)

  las_data_files = list(glob.iglob(f"{args.directory}/*.las", recursive=True))
  logging.info(f"Found {len(las_data_files)} las data files")
 
  las_data_files = las_data_files

  total_docs = 0
  total_files = 0
  this_files_docs = 0
  failed = 0

  logging.info(f'Parsing and indexing data into the {args.index} index in the Elasticsearch cluster at {full_es_url}')

  for i, doc in enumerate(las_data_files):
    logging.info(f"Processing file {i + 1}/{len(las_data_files)}")
    result = parse_las_data(doc, args.index, es)

    if result is False:
      logging.info("Failed to parse file... doesn't follow the provide sample format")
      failed = failed + 1

  logging.info(f'{len(las_data_files)} LAS files processed and indexed into Elasticsearch.')
  logging.info(f'{failed} LAS files failed when uploading as they didnt follow the sample format')

