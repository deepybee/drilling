#!/usr/bin/env python3

import pandas as pd
from LAS import Converter
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from ssl import create_default_context
import re
import argparse

def parse_las_data(las_data_doc, index_name, es):
    c = Converter()
    # Download the LAS file from S3
    s3.Object(target_bucket.name, las_data_doc).download_file(f'/tmp/las_processing.las')

    # Read it
    log = c.set_file('/tmp/las_processing.las')

    meta_data = {}
    curve_data = {}
    data = {}

    # Programatically get all meta data fields and store them nicely
    for meta_key, meta_value in log.get_dict()['well'].items():
      if meta_value is not None:
        meta_data[meta_value['desc'].replace(' ', '_').lower()] = meta_value['value']

    # Programatically get all curve data and names and store them nicely
    for curve_key, curve_value in log.get_dict()['curve'].items():
      curve_data[curve_key] = {
        "name": curve_value['desc'].split("  ")[1].replace(' ', '_').lower(),
        "unit": curve_value['unit']
      }
      # Get the actual curve data and store it in a dict for Pandas to read
      data[curve_key.lower()] = log.get_dict()['data'][curve_key.lower()]

    # Read the curve data into pandas which automagically tidies a lot up
    las_df = pd.DataFrame(data)

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
    print(f"Uploading {len(all_data)}")
    helpers.bulk(es, all_data)

    print(f'Indexed {len(all_data)} records from LAS data file {str(las_data_doc)}')

if __name__ == "__main__":
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
  parser.add_argument('--bucket', metavar='my-bucket', type=str,
                     help='name of the S3 bucket to retrieve files from', required=True)


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
      es = Elasticsearch(full_es_url, http_auth=(args.user, args.password))


  if es.indices.exists(index=args.index) is False:
    es.indices.create(index=args.index, body=index_mapping)

  s3 = boto3.resource('s3')

  target_bucket = s3.Bucket(name=args.bucket)

  las_data_pattern = re.compile('\d+\.las')
  las_data_files = []

  print('\nTraversing S3 bucket', str(target_bucket.name), 'for LAS data files, please wait')

  for s3_object in target_bucket.objects.all():
      if re.search(las_data_pattern, str(s3_object)):
          las_data_files.append(s3_object.key)


  print('\nFound ' + str(len(las_data_files)) + ' LAS data files in S3 bucket ' + str(target_bucket.name), '\n')

  total_docs = 0
  total_files = 0
  this_files_docs = 0

  print('Parsing and indexing data into the', args.index, 'index in the Elasticsearch cluster at', full_es_url, '\n')

  for i, doc in enumerate(las_data_files):
    print(f"Processing file {i + 1}/{len(las_data_files)}")
    parse_las_data(doc, args.index, es)

  print()
  print(f'{len(las_data_files)} LAS files processed and indexed into Elasticsearch.')


