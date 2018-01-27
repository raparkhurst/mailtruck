#!/usr/bin/env python3

import optparse
import sys
import elasticsearch
import requests
import pprint

version="0.0.1"



parser = optparse.OptionParser()
parser.add_option('--verbose',
                  help="enable verbose reporting",
                  action="store_true",
                  default=False,
                  dest="verbose")

parser.add_option('-v',
                  help="display version information",
                  action="store_true",
                  default=False,
                  dest="version")

parser.add_option("--esCluster",
                  help="Name of Elastic Search Cluster",
                  dest="es_cluster",
                  default="localhost")

parser.add_option("--ls",
                  help="list indicies",
                  action="store_true",
                  dest="es_ls",
                  default=False)

parser.add_option("--label",
                  help="Change label of an index.  MUST BE USED with esCluster AND esIndex",
                  dest="es_label")

parser.add_option("--esIndex",
                  help="ES Index to change Newman Label of",
                  dest="es_index")


options, remainder = parser.parse_args()
parser.parse_args()
pp = pprint.PrettyPrinter(indent=4)


def ls_indices():
    print("Index => Newman Label Map:")
    for idx in es.indices.get('*'):
        res = es.search(index=idx, body={"query":{"bool":{"must":[{"query_string":{"default_field":"_all","query":idx}}],"must_not":[],"should":[]}},"from":0,"size":1,"sort":[],"aggs":{}})
        label = res['hits']['hits'][0]['_source']['label']

        print("\t" + idx + "\t=>\t" + label)


def es_update(es_index, label_name):
    es.update(index=es_index,
              doc_type=es_index,
              id=es_index,
              body={"doc": { "label": label_name}})


def print_version_info():
    print("version is:  " + version)



if __name__ == "__main__":

    # note:  need to add error checking for ensuring elastic cluster is specified.
    # right now it will just error out!


    if (options.verbose):
        print("Elasticsearch module is:  " + str(elasticsearch.__version__))
        pp.pprint(res.content)
    if (options.version):
        print_version_info()
        sys.exit(0)

    # not the cleanest but works for now.  if we expand on this we should make it cleaner
    try:
        res = requests.get("http://" + options.es_cluster + ":9200")
        es = elasticsearch.Elasticsearch([{'host': options.es_cluster, 'port': '9200'}])
    except Exception as ex:
        print("Error:  Unable to connect to elasticsearch cluster!", ex)
        sys.exit(0)


    if (options.es_ls):
        ls_indices()
    elif (options.es_label):
        try:
            es_update(es_index=options.es_index, label_name=options.es_label)
        except Exception as ex:
            print("Error -- check that you specified a label and index!")
    else:
        print("nothing to do")
