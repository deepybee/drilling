# drilling
Ingest parsers for Elasticsearch which cater for esoteric log formats in the drilling industry

`eLAStify_s3.py` scrapes an S3 bucket for LAS files then downloads and parses them before indexing them into Elasticsearch.

```bash
./eLAStify_s3.py --help
usage: eLAStify.py [-h] --es-url elasticsearch.mydomain.com [--es-port 9200]
                   [--index las_data] [--insecure False] [--user elastic]
                   [--password Pa55w0rd] [--ca-cert /path/to/ca/ca.crt]
                   --bucket my-bucket

Processes LAS data files and indexes them into an Elasticsearch cluster.

optional arguments:
  -h, --help            show this help message and exit
  --es-url elasticsearch.mydomain.com
                        the Elasticsearch cluster URL
  --es-port 9200        the port Elasticsearch listens on
  --index las_data      the Elasticsearch cluster URL
  --insecure False      set to True (not lowercase true) if connecting to a
                        cluster not secured with SSL/TLS
  --user elastic        not needed if connecting to an unsecured cluster
  --password Pa55w0rd   not needed if connecting to an unsecured cluster
  --ca-cert /path/to/ca/ca.crt
                        not needed if connecting to a cluster not secured with
                        SSL/TLS
  --bucket my-bucket    name of the S3 bucket to retrieve files from
```

`top2es_s3.py` performs the same function but on Wellbore / TOPS data format files.

```bash
./top2es_s3.py --help  
usage: top2es.py [-h] --es-url elasticsearch.mydomain.com [--es-port 9200]
                 [--index tops_data] [--insecure False] [--user elastic]
                 [--password Pa55w0rd] [--ca-cert /path/to/ca/ca.crt] --bucket
                 my-bucket

Processes Wellbore / TOPS data files and indexes them into an Elasticsearch
cluster.

optional arguments:
  -h, --help            show this help message and exit
  --es-url elasticsearch.mydomain.com
                        the Elasticsearch cluster URL
  --es-port 9200        the port Elasticsearch listens on
  --index tops_data     the index the documents should be written to
  --insecure False      set to True (not lowercase true) if connecting to a
                        cluster not secured with SSL/TLS
  --user elastic        not needed if connecting to an unsecured cluster
  --password Pa55w0rd   not needed if connecting to an unsecured cluster
  --ca-cert /path/to/ca/ca.crt
                        not needed if connecting to a cluster not secured with
                        SSL/TLS
  --bucket my-bucket    name of the S3 bucket to retrieve files from

```

the `_localfile` variants perform the same functions on a local directory; for these replace the `--bucket` flag with `--directory` and pass the desired location to search (NB: search is recursive so subfolders will also be checked for matching files).