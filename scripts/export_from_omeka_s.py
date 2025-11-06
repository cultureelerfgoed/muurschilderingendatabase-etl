"""Script that downloads linked data from Omeka S API and fixes namespace issues."""

import os
import logging
from ssl import SSLCertVerificationError, SSLError
import requests
from rdflib import Graph, URIRef
from rdflib.namespace import RDF

### Configuration
# Path to save file
TARGET_FILEPATH = os.getenv('TARGET_FILEPATH', 'data/api-export.ttl')
# URI to Omeka S endpoint
BASE_URI = os.getenv('BASE_URI', 'https://muurschilderingendatabase.nl/')
# Defines the format of the output file
OUTPUT_FILE_FORMAT = os.getenv('OUTPUT_FILE_FORMAT', 'ttl')
# Defines the graph identifier
GRAPH_ID = os.getenv('GRAPH_ID', 'default')
# Set encoding
ENCODING = os.getenv('ENCODING', 'utf-8')
### End of Configuration

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

try:
    graph = Graph(identifier=GRAPH_ID)

    # retrieve items fron api endpoint
    logger.info("Retrieving items from API endpoint %s", BASE_URI)
    logger.info("Writing to %s", TARGET_FILEPATH)

    # Get triples from API endpoint and write it to a file.
    with open(TARGET_FILEPATH, "w", encoding=ENCODING) as file:
        for page in range(1,100):
            PAGE_URL = f"{BASE_URI}api/items?format=turtle&page={page}&per_page=100"
            data = requests.get(PAGE_URL, timeout=90)
            
            if(data.apparent_encoding != "ascii"):
                file.write(data.text)
            else:
                logger.info("Data from API ended on page: %s", page)
                break

        # Parse the written file as a graph
        graph.parse(source=TARGET_FILEPATH, format=OUTPUT_FILE_FORMAT)

        # Filter out broken triples.
        for subj, pred, obj in graph:
            if ("@context" in subj or "@context" in obj) or (graph[obj: RDF.type] and "customvocab" in graph[obj: RDF.type]):
                logger.warning("Removing an unserializable triple:")
                logger.warning("- Subject: %s", subj)
                logger.warning("- Predicate: %s", pred)
                logger.warning("- Object: %s", obj)
                graph.remove((subj, pred, obj))            

        # Retrieve namespaces from api-context endpoint and bind them
        namespace_response = requests.get(BASE_URI+"api-context", timeout=200)
        namespace_data = namespace_response.json()["@context"]

        for key in namespace_data:
            ns = URIRef(namespace_data[key].replace('\\', ''))
            logger.info("Binding namespace  %s", f"{ns} as {key}")
            graph.namespace_manager.bind(key, ns, override=True, replace=True)

        # Serialize graph
        logger.info("Writing  %s", f"{OUTPUT_FILE_FORMAT} file to {TARGET_FILEPATH}")
        graph.serialize(format=OUTPUT_FILE_FORMAT, destination=TARGET_FILEPATH, encoding=ENCODING, auto_compact=True)
        logger.info("Filesize:  %s", f"{os.path.getsize(TARGET_FILEPATH)} bytes")
except (SSLCertVerificationError, SSLError) as ssle:
    logger.error("Caught SSLError/SSLCertVerificationError %s", str(ssle))
finally:
    logger.info("Exiting..")
