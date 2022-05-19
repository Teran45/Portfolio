from elasticsearch import Elasticsearch
es = Elasticsearch(hosts=['http://127.0.0.1:9200'])

es.indices.delete(index='terminals', ignore=[400, 404])
es.indices.delete(index='key_logs', ignore=[400, 404])
es.indices.delete(index='report', ignore=[400, 404])
es.indices.delete(index='transactions', ignore=[400, 404])
