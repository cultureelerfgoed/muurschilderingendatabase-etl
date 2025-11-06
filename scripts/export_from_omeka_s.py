"""Script that downloads linked data from Omeka S API and fixes namespace issues."""

import os
import logging
from ssl import SSLCertVerificationError, SSLError
import requests
from rdflib import Graph, URIRef
from rdflib.namespace import RDF


### Configuration
# Path to save file
#EXPORT_PATH = "data/api-export.ttl"
TARGET_FILEPATH = os.getenv('TARGET_FILEPATH', 'data/api-export.ttl')
# URI to Omeka S endpoint
#BASE_URI = "https://muurschilderingendatabase.nl/"
BASE_URI = os.getenv('BASE_URI', 'https://muurschilderingendatabase.nl/')
# Defines the format of the output file
OUTPUT_FILE_FORMAT = "ttl"
# Defines the graph identifier
GRAPH_ID = "muurschildering-origineel"
### End of Configuration

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    filename='muurschilderingendatabase-etl.log',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

try:
    graph = Graph(identifier=GRAPH_ID)

    # retrieve items fron api endpoint
    logger.info("Retrieving items from API endpoint %s", BASE_URI, exc_info=True)
    logger.info("Writing file to  %s", TARGET_FILEPATH, exc_info=True)

    with open(f"{TARGET_FILEPATH}.{OUTPUT_FILE_FORMAT}", "w", encoding="utf-8") as file:
        # Temporary subset to make testing faster
        for page in range(1,75):
            PAGE_URL = f"{BASE_URI}api/items?format=turtle&page={page}&per_page=100"
            data = requests.get(PAGE_URL, timeout=200)
            file.write(data.text)

        graph.parse(f"{TARGET_FILEPATH}.{OUTPUT_FILE_FORMAT}")

        # Filter out broken triples.
        for subj, pred, obj in graph:
            if "@context" in subj or "@context" in obj:                
                logger.info("Removing an unserializable triple  %s", f"s: {subj} p: {pred} o: {obj}", exc_info=True)
                graph.remove((subj, pred, obj))
            
            if graph[obj: RDF.type] and "customvocab" in graph[obj: RDF.type]:
                print(f"s: {subj} p: {pred} o: {obj}")

        # retrieve namespaces from api-context endpoint
        namespace_response = requests.get(BASE_URI+"api-context", timeout=200)
        namespace_data = namespace_response.json()["@context"]

        for key in namespace_data:
            ns = URIRef(namespace_data[key].replace('\\', ''))
            logger.info("Binding namespace  %s", f"{ns} as {key}", exc_info=True)
            graph.namespace_manager.bind(key, ns, override=True, replace=True)

        logger.info("Writing  %s", f"{OUTPUT_FILE_FORMAT} file to {TARGET_FILEPATH}", exc_info=True)
        graph.serialize(format=OUTPUT_FILE_FORMAT, destination=f"{TARGET_FILEPATH}")

except (SSLCertVerificationError, SSLError):
    logger.error("Caught SSLCertVerificationError %s", f"{SSLCertVerificationError.strerror} due to {SSLCertVerificationError.reason}", exc_error=True)    
finally:
    logger.info("Exiting..")
