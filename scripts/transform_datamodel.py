"""Script that downloads linked data from Omeka S API and fixes namespace issues."""

import os
import logging
import requests
from rdflib import Graph, URIRef
from rdflib.namespace import DCTERMS
from rdflib.plugins.parsers.notation3 import BadSyntax

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

### Mapping the term to be replaced on the left by the term on the right. 
mapping = {}

### End of Mapping 

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',                    
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger.info("Retrieving items from %s", TARGET_FILEPATH)

try: 
    # Load graph from file
    graph = Graph(identifier=GRAPH_ID)
    graph.parse(source=TARGET_FILEPATH, format=OUTPUT_FILE_FORMAT)
    enrichment_graph = Graph(identifier="enrichment_graph")
    
    with open("data/enrichments.ttl", "w", encoding=ENCODING) as enrichmentsfile:
         
        # Apply enrichments to graph
        for subj, pred, obj in graph:

            # Enrich rijksmonumenten via URI        
            if 'Rijksmonument' in obj and graph[subj : DCTERMS.identifier] is not None:
                for item in graph[subj : DCTERMS.identifier]:
                    if not isinstance(item, URIRef) and "RM" in item[0:2]:
                        RM_URI=f"https://api.linkeddata.cultureelerfgoed.nl/queries/rce/rest-api-rijksmonumenten/run?rijksmonumentnummer={item[2:]}"
                        data = requests.get(RM_URI, timeout=200)
                        #logger.info("Adding enrichment for Rijksmonumentnummer: %s", str(item[2:]))                
                        enrichmentsfile.write(data.text)
        
        logger.info("Parsing graph..")
        enrichment_graph.parse("data/enrichments.ttl")
        logger.info("Loaded %s triples", str(len(enrichment_graph)))

        for subj, pred, obj, in enrichment_graph:
            if "https://linkeddata.cultureelerfgoed.nl/def/ceo#rijksmonumentnummer" in pred:
                graph.add((subj, obj, pred))
                logger.info("Adding enrichment: %s", (subj, pred, obj))
            
    # Apply mapping to graph
    for subj, pred, obj in graph:

        if pred in mapping.keys():
            graph.remove((subj, pred, obj))
            graph.add((subj, mapping[pred], obj))        

    # Serialize graph
    logger.info("Writing %s", f"{OUTPUT_FILE_FORMAT} file to {TARGET_FILEPATH}")
    graph.serialize(format=OUTPUT_FILE_FORMAT, destination=TARGET_FILEPATH, encoding=ENCODING, auto_compact=True)
    logger.info("Filesize: %s", f"{os.path.getsize(TARGET_FILEPATH)} bytes")
except BadSyntax as bs:
    logger.error("Error loading graph: %s", str(bs))
