import requests
import json
import math
import argparse
import logging

# Method to get index definition from src_index and create a dst_index with the input JSON (same filterable fields, analyzers, etc)
def create_dst_index(src_endpoint, src_index, dst_index, src_headers, dst_headers):    
    api_version = '?api-version=2021-04-30-Preview'
    urlsrc = src_endpoint + "indexes/" + src_index + api_version
    # For example with this REST API call you get the index definition: GET https://{{search_service}}.search.windows.net/indexes/{{index_name_src}}?api-version=2021-04-30-Preview
    # This REST API call enables to create an index with a given definition definition PUT https://{{search_service}}.search.windows.net/indexes/{{index_name}}?api-version=2021-04-30-Preview
    # url = src_endpoint + "indexes/" + dst_index + searchstring + api_version 
    response  = requests.get(urlsrc, headers=src_headers)
    indexdef = response.json()
    urldst = src_endpoint + "indexes/" + dst_index + api_version
    indexdef.pop('name', None) # removing the name property that contains old name from src_index inside the json
    response = requests.put(urldst, headers = dst_headers, json = indexdef)
    #print(response.text)

# Define next chunk to process in the batch
def get_next_chunk(current_val):    
    return chr(ord(current_val) + 1), chr(ord(current_val) + 1) , chr(ord(current_val) + 2)

# Read all documents in a exported batch [low_b]
def read_all_docs(low_b):
    documents = []
    f = open ('export_'+low_b+'.txt', "r")    
    documents = json.loads(f.read())
    return documents

# Get all documents in a batch [low_b, high_b]
def get_all_docs(low_b, high_b, src_endpoint, src_headers, filter_by= "Id"):
    documents = []
    searchstring = f"&$filter={filter_by} gt '{low_b}' and {filter_by} le '{high_b}'&$count=true"
    url = src_endpoint + "indexes/" + src_index +"/docs" + api_version + searchstring
    response  = requests.get(url, headers=src_headers, json=searchstring)
    query = response.json()

    if query.get('value') != None:
        for doc in query.get('value'):
            documents.append(doc)
            # Continue if needed
        while('@odata.nextLink' in query.keys()):
            next_link = query['@odata.nextLink']
            #print(next_link)
            response = requests.get(next_link, headers=src_headers)
            query = response.json()
            for doc in query.get('value'):
                documents.append(doc)
        #print(query)
    return documents

# Export all documents in a batch [low_b, high_b] into a JSON
def export_all_docs_in_batch(low_b, high_b, src_endpoint, src_headers, filter_by= "Id"):
    documents = []
    searchstring = f"&$filter={filter_by} gt '{low_b}' and {filter_by} le '{high_b}'&$count=true"
    url = src_endpoint + "indexes/" + src_index +"/docs" + api_version + searchstring
    response  = requests.get(url, headers=src_headers, json=searchstring)
    query = response.json()

    if query.get('value') != None:

        for doc in query.get('value'):
            documents.append(doc)
            # Continue if needed
        while('@odata.nextLink' in query.keys()):
            next_link = query['@odata.nextLink']
            #print(next_link)
            response = requests.get(next_link, headers=src_headers)
            query = response.json()
            for doc in query.get('value'):
                documents.append(doc)
        #print(query)    
    with open('export_'+low_b+'.txt', 'w') as outfile:
        json.dump(documents, outfile)

def push_docs(all_documents, dst_endpoint, dst_headers):
    # Push data
    batch_index = 0
    while batch_index * 50 < len(all_documents):
        batch_start = batch_index * 50
        batch_end = (batch_index + 1) * 50 if (batch_index + 1) * 50 < len(all_documents) else len(all_documents)
        logging.info(f"Pushing batch #{batch_index} [{batch_start},{batch_end}]")
        push_batch(all_documents[batch_start:batch_end], dst_endpoint, dst_headers)
        batch_index += 1
        # print(index_content)

def push_batch(batch_documents, dst_endpoint, dst_headers):
    search_docs = {
        "value" : []
    }
    for d in batch_documents:
        del d['@search.score'] 
        d['@search.action'] = 'mergeOrUpload'
        search_docs['value'].append(d)
    # search_docs['value'][0]
    url = dst_endpoint + "indexes/" + dst_index + '/docs/index' + api_version
    response = requests.post(url, headers = dst_headers, json = search_docs)
    index_content = response.json()
    #logging.info(response.status_code)

def count_docs(dst_endpoint, dst_index, dst_headers):
    # Check number of docs in destination index
    searchstring = '&$count=true'
    url = dst_endpoint + "indexes/" + dst_index +"/docs" + api_version + searchstring
    response  = requests.get(url, headers=dst_headers)
    query = response.json()
    docCount = query['@odata.count']
    logging.info(f"Found {docCount} documents in destination index")

# MAIN
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("debug.log"),
            logging.StreamHandler()
        ]
    )
    parser = argparse.ArgumentParser(description='Input parameter for azure-search-backup-index')
    parser.add_argument('--src_service', dest="src_service", required=True,
                        help='Azure Cognitive Search SOURCE Service')
    parser.add_argument('--dst_service', dest="dst_service",
                        help='Azure Cognitive Search DESTINATION Service')
    parser.add_argument('--src_service_key', dest="src_service_key", required=True,
                        help='Azure Cognitive Search SOURCE Service KEY')
    parser.add_argument('--dst_service_key', dest="dst_service_key",
                        help='Azure Cognitive Search DESTINATION Service KEY')
    parser.add_argument('--src_index', dest="src_index", required=True,
                        help='Azure Cognitive Search SOURCE Index')
    parser.add_argument('--dst_index', dest="dst_index", required=True,
                        help='Azure Cognitive Search DESTINATION Index')                           
    parser.add_argument('--filter_by', dest="filter_by", required=True,
                        help='Field within the index to filter by')
    parser.add_argument('--action', dest="action", required=True,
                        help='Action to perform, either export or import a backup or just duplicate an index into a new one. Enter export, import or duplicate')

    args = parser.parse_args()
    src_service = args.src_service
    dst_service = src_service if args.dst_service is None else args.dst_service
    src_service_key = args.src_service_key
    dst_service_key = src_service_key if args.dst_service_key is None else args.dst_service_key
    src_index = args.src_index
    dst_index = args.dst_index
    filter_by = args.filter_by
    action = args.action
    src_endpoint = 'https://' + src_service + '.search.windows.net/'
    dst_endpoint = 'https://' + dst_service + '.search.windows.net/'
    api_version = '?api-version=2021-04-30-Preview'
    src_headers = {'Content-Type': 'application/json',
            'api-key': src_service_key } 
    dst_headers = {'Content-Type': 'application/json',
            'api-key': dst_service_key }
    if action == 'export':
        val = '/'
        while (ord(val) <= 123):
            val, low_b, high_b = get_next_chunk(val)
            logging.info(f" Processing interval : [{low_b},{high_b}]")
            export_all_docs_in_batch(low_b, high_b, src_endpoint, src_headers, filter_by=filter_by)
    if action == 'import':
        create_dst_index(src_endpoint, src_index, dst_index, src_headers, dst_headers)
        logging.info(f"Creating {dst_index} and importing the backups from your current folder")
        val = '/'
        while (ord(val) <= 123): 
            val, low_b, high_b = get_next_chunk(val)
            logging.info(f" Importing interval : [{low_b},{high_b}]")
            documents = []
            documents = read_all_docs(low_b)
            push_docs(documents, dst_endpoint, dst_headers)
        count_docs(dst_endpoint, dst_index, dst_headers)
    if action == 'duplicate':
        create_dst_index(src_endpoint, src_index, dst_index, src_headers, dst_headers)
        val = '/'
        while (ord(val) <= 123):
            val, low_b, high_b = get_next_chunk(val)
            logging.info(f" Processing interval : [{low_b},{high_b}]")
            documents = []
            documents = get_all_docs(low_b, high_b, src_endpoint, src_headers, filter_by=filter_by)
            logging.info(len(documents))
            push_docs(documents, dst_endpoint, dst_headers)
        count_docs(dst_endpoint, dst_index, dst_headers)

# Possible concerns/future improvements:
#    - should the source and destination have the same size within index? ie not only number of docs but also field content
#    - what happens with non retrievable for searchable fields (they wouldnt be exported in the backup)
#    - to do: segment export files to handle massive index (possibly using the batch size)
#    - pending to implement parallel push ingestion with exponential retry policies
