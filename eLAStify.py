#!/usr/bin/env python3

import pandas as pd
from LAS import Converter
from elasticsearch import Elasticsearch
from ssl import create_default_context
import re
import boto3
import argparse

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

if args.insecure:
    full_es_url = 'http://' + args.es_url + ':' + str(args.es_port)
    es = Elasticsearch(full_es_url)

else:
    full_es_url = 'https://' + args.es_url + ':' + str(args.es_port)
    context = create_default_context(cafile=args.ca_cert)
    es = Elasticsearch(full_es_url, ssl_context=context, http_auth=(args.user, args.password))

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

def parse_las_data(las_data_doc):

    s3.Object(target_bucket.name, las_data_doc).download_file(f'/tmp/las_processing.las')

    c = Converter()

    log = c.set_file('/tmp/las_processing.las')

    dict_from_las = log.get_dict()


    def parse_las_data(top_data_doc):
        pass

    data = dict_from_las['data']
    las_columns = {'depth': data['dept'],
                   'caliper': data['cali'],
                   'bulk_density': data['den'],
                   'delta_t_compressional': data['dt'],
                   'neutron_porosity_in_limestone_units': data['neu'],
                   'resistivity_shallow': data['resslw'],
                   'resistivity_deep': data['res_dep_ind'],
                   'spontaneous_potential': data['sp'],
                   'spontaneous_potential_corrected': data['spc']
                   }

    las_df = pd.DataFrame(las_columns)
    las_df['latitude'] = dict_from_las['well']['LATI']['value']
    las_df['longitude'] = dict_from_las['well']['LONG']['value']
    las_df['geo_point'] = las_df['latitude'].astype(str) + "," + las_df['longitude'].astype(str)
    las_df['field_name'] = dict_from_las['well']['FLD']['value']
    las_df['country'] = dict_from_las['well']['CTRY']['value']
    las_df['operator'] = dict_from_las['well']['COMP']['value']


    def frame2doc(dataframe):
        global this_files_docs
        this_files_docs = 0
        body = []
        for row in dataframe.index:
            body.append({'index': {'_index': args.index, '_type': '_doc'}})
            body.append(dataframe.loc[row].to_json())

            global  total_docs
            total_docs += 1

            this_files_docs += 1

        response = es.bulk(body=body)

    frame2doc(las_df)

    print('Indexed', str(this_files_docs), 'documents from LAS data file', str(las_data_doc))

    global total_files
    total_files += 1
if __name__ == "__main__":
    for doc in las_data_files:
        parse_las_data(doc)
    print('\nTotal', str(total_docs), 'documents processed and indexed into Elasticsearch from', str(total_files), 'source files.')