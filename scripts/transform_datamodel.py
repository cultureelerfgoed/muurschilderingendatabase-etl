"""Script that downloads linked data from Omeka S API and fixes namespace issues."""

import os
import logging
import requets
from rdflib import Graph, URIRef
from rdflib.namespace import SDO, DCTERMS, OWL
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
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',                    
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger.info("Retrieving items from %s", TARGET_FILEPATH)

try: 
    # Load graph from file
    graph = Graph(identifier=GRAPH_ID)
    graph.parse(source=TARGET_FILEPATH, format=OUTPUT_FILE_FORMAT)
    old_g_length = len(graph)

    # Apply enrichments to graph
    for subj, pred, obj in graph:

        # Enrich rijksmonumenten via URI
        with open("data/enrichments.ttl", "w", encoding=ENCODING) as enrichmentsfile:
            if 'Rijksmonument' in obj and graph[subj : DCTERMS.identifier] is not None:
                for item in graph[subj : DCTERMS.identifier]:
                    if not isinstance(item, URIRef) and "RM" in item[0:2]:
                        RM_URI=f"https://api.linkeddata.cultureelerfgoed.nl/queries/rce/rest-api-rijksmonumenten/run?rijksmonumentnummer={item[2:]}"
                        data = requests.get(RM_URI, timeout=200)
                        logger.info("Adding enrichment for Rijksmonumentnummer: %s", str(item[2:]))                
                        enrichmentsfile.write(data.text)                        
        
        graph.parse(source="data/enrichments.ttl")

    # Apply mapping to graph
    for subj, pred, obj in graph:

        if pred in mapping.keys():
            graph.remove((subj, pred, obj))
            graph.add((subj, mapping[pred], obj))        

    new_g_length = len(graph)

    # Test that the new graph contains as many triples as the old graph
    assert len(graph) == old_g_length

    # Serialize graph
    logger.info("Writing %s", f"{OUTPUT_FILE_FORMAT} file to {TARGET_FILEPATH}")
    graph.serialize(format=OUTPUT_FILE_FORMAT, destination=TARGET_FILEPATH, encoding=ENCODING, auto_compact=True)
    logger.info("Filesize: %s", f"{os.path.getsize(TARGET_FILEPATH)} bytes")
except BadSyntax as bs:
    logger.error("Error loading graph: %s", str(bs))
