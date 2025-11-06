"""Script that downloads linked data from Omeka S API and fixes namespace issues."""

import os
import logging
from rdflib import Graph
#from rdflib.namespace import CEO
from rdflib.namespace import SDO, DCTERMS, OWL 

### Configuration sdaf
# Path to save file
TARGET_FILEPATH = os.getenv('TARGET_FILEPATH', 'data/api-export.ttl')
# URI to Omeka S endpoint
BASE_URI = os.getenv('BASE_URI', 'https://muurschilderingendatabase.nl/')
# Defines the format of the output file
OUTPUT_FILE_FORMAT = "ttl"
# Defines the graph identifier 
GRAPH_ID = "muurschildering-nieuw"
### End of Configuration

### Mapping the term to be replaced on the left with the one on the right. 

mapping = {
    DCTERMS.title: SDO.name,
    DCTERMS.publisher: SDO.publisher,
    DCTERMS.identifier: SDO.identifier,
    DCTERMS.issued: SDO.dateIssued,
    DCTERMS.abstract: SDO.abstract,
    DCTERMS.creator: SDO.creator,
    DCTERMS.bibliographicCitation: SDO.citation,
    DCTERMS.created: SDO.dateCreated,
    DCTERMS.description: SDO.description,
    DCTERMS.isReferencedBy: SDO.subjectOf,
    OWL.sameAs: SDO.sameAs    
}

### End of Mapping 

logger = logging.getLogger(__name__)
logging.basicConfig(filename='muurschilderingendatabase-etl.log', level=logging.INFO)
logger.info(f"Retrieving items from {TARGET_FILEPATH}")

graph = Graph(identifier=GRAPH_ID)
graph.parse(TARGET_FILEPATH)
old_g_length = len(graph)

### Do stuff to graph ###
for subj, pred, obj in graph:

    if pred in mapping.keys():
        graph.remove((subj, pred, obj))
        graph.add((subj, mapping[pred], obj))        

new_g_length = len(graph)

# Test that the new graph contains as many triples as the old graph
assert len(graph) == old_g_length

### Write graph ###
logger.info(f"Writing {OUTPUT_FILE_FORMAT} file to {TARGET_FILEPATH}")
graph.serialize(format=OUTPUT_FILE_FORMAT, destination=f"{TARGET_FILEPATH}")