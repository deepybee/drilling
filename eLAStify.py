#!/usr/bin/env python3

import pandas as pd

from LAS import Converter

c = Converter()

log = c.set_file("local_file.las")

dict_from_las = log.get_dict()

from elasticsearch import Elasticsearch

from ssl import create_default_context

context = create_default_context(cafile="/path/to/ca.crt")

es = Elasticsearch("https://localhost:9200", ssl_context=context, http_auth=('elastic', 'Pa55w0rd'))

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
    body = []
    for row in dataframe.index:
        body.append({'index': {'_index': 'python_las_test', '_type': '_doc'}})
        body.append(dataframe.loc[row].to_json())

    response = es.bulk(body=body)

if __name__ == "__main__":
    frame2doc(las_df)