import argparse
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

def generate_bulk_actions(file, index, pipeline, skip_first_line, max_docs):
    with open(file, encoding = 'UTF-8') as f:
        for idx, line in enumerate(f.readlines()):
            if skip_first_line and idx ==0:
                continue
            if max_docs != -1:
                stop_idx = max_docs + 1 if skip_first_line else max_docs
                if idx == stop_idx:
                    break
            action = {
                '_index': index,
                '_source': {
                    'message': line.strip()
                }
            }
            if pipeline:
                action['pipeline'] = pipeline
            yield action

def main():
    parser = argparse.ArgumentParser(description='Index lines from a specified file into Elasticsearch')
    parser.add_argument('file', metavar='file', help='The file to read')
    parser.add_argument('index', metavar='index_name', help='Where to index documents from file')
    parser.add_argument('-p', '--pipeline', dest='pipeline', help='The pipeline to use when indexing documents')
    parser.add_argument('-e', '--elasticsearch-host', dest='host', help='The Elasticsearch host (default is http://localhost:9200)')
    parser.add_argument('-s', '--skip-first-line', dest='skip_first_line', action='store_true', help='If set, do not process the first line of the file (typically the header in a CSV file)')
    parser.add_argument('-m', '--max-docs', dest='max_docs', type=int, help='The maximum number of lines to process')

    args = parser.parse_args()

    es_host = args.host or 'http://localhost:9200'

    es = Elasticsearch([es_host])
    print('-- elasticsearch host set to :', es_host)

    successes = 0
    for ok, _ in streaming_bulk(client=es, actions= generate_bulk_actions(args.file, args.index, args.pipeline or '', 
    args.skip_first_line, args.max_docs or -1)):
        successes += ok
    
if __name__ == "__main__":
    main()