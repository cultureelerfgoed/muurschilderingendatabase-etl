"""Script that transforms and enriches linked data."""

import os
import logging
import requests
import uritools
from rdflib import Graph, URIRef
import rdflib.namespace as ns_module
from rdflib.namespace import DCTERMS, RDF
from rdflib.plugins.parsers.notation3 import BadSyntax

# --- Configuration ---
TARGET_FILEPATH = os.getenv('TARGET_FILEPATH', 'api-export.ttl')
BASE_URI = os.getenv('BASE_URI', 'https://muurschilderingendatabase.nl/')
OUTPUT_FILE_FORMAT = os.getenv('OUTPUT_FILE_FORMAT', 'ttl')
GRAPH_ID = os.getenv('GRAPH_ID', 'default')
ENCODING = os.getenv('ENCODING', 'utf-8')

# --- Logging ---
logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Functions ---
def get_filter_from_env() -> list[URIRef]:
    """Load mapping from environment variables with dynamic namespace resolution."""
    filterlist = []

    for key, value in os.environ.items():
        if key.startswith('FILTER'):
            if uritools.is_valid_uri(value):
                filterlist.append(URIRef(value))
            else:
                logger.warning('Skipping invalid filter entry %s', f'{key}={value}')

    return filterlist
    
def import_namespace_by_name(ns_name: str):
    """Safely get a namespace, validating it's actually a namespace."""
    ns = getattr(ns_module, ns_name)

    # Verify it looks like a namespace (has _NS suffix in rdflib)
    if not hasattr(ns, '_NS'):
        raise ValueError(f"{ns_name} is not a valid namespace")

    return ns

def get_mapping_from_env() -> dict[str, str]:
    """Load mapping from environment variables with dynamic namespace resolution."""
    mapping = {}

    for key, value in os.environ.items():
        if key.startswith('MAP_'):

            # Parse source predicate (e.g., "DCTERMS.title")
            src_parts = key[4:].split('_')
            src_ns_name = src_parts[0]
            src_pred_name = src_parts[1].lower()
            # Parse target predicate (e.g., "SDO.name")
            tgt_ns_name, tgt_pred_name = value.split('.')

            # Dynamically import namespaces
            src_ns = import_namespace_by_name(src_ns_name)
            tgt_ns = import_namespace_by_name(tgt_ns_name)

            if not src_ns or not tgt_ns:
                raise ValueError(f"Unknown namespace in {key}={value}")

            # Get predicates
            src_pred = getattr(src_ns, src_pred_name)
            tgt_pred = getattr(tgt_ns, tgt_pred_name)

            logger.info("Adding entry to transform mapping %s to %s.", f"{src_ns_name}.{src_pred_name}", f"{tgt_ns}.{tgt_pred}")
            mapping[src_pred] = tgt_pred

    return mapping

def load_graph(filepath: str, format: str) -> Graph:
    """Load RDF graph from file."""
    graph = Graph(identifier=GRAPH_ID)
    graph.parse(source=filepath, format=format)
    logger.info(f"Loaded graph with {len(graph)} triples")
    return graph

def enrich_with_rijksmonument_data(graph: Graph) -> None:
    """Enrich graph with Rijksmonument data."""
    with open("enrichments.ttl", "w", encoding=ENCODING) as f:
        rm_list = list(graph.triples((None, RDF.type, None)))
        logger.info('trying to enrich %i Rijksmonumenten', len(list(rm_list)))
        for subj, pred, obj in rm_list: 
            for subj_id, pred_ic, obj_id, in graph.triples((subj, DCTERMS.identifier, None)):
                if not isinstance(str(obj_id), URIRef) and "RM" in str(obj_id)[0:2]:
                    rm_uri = f"https://api.linkeddata.cultureelerfgoed.nl/queries/rce/rest-api-rijksmonumenten/run?rijksmonumentnummer={str(obj_id)[2:]}"
                    try:
                        data = requests.get(rm_uri, timeout=200)
                        f.write(data.text)
                    except requests.RequestException as e:
                        logger.error("Failed to fetch %s", f"{rm_uri}: {e}")
    graph.parse("enrichments.ttl")

def apply_mapping(graph: Graph, mapping: dict):
    """Apply predicate mappings to the graph."""
    if mapping: 
        for subj, pred, obj in list(graph):  # Use list() to avoid modification during iteration
            if pred in mapping:
                graph.remove((subj, pred, obj))
                graph.add((subj, mapping[pred], obj))
    logger.info(f"Applied {len(mapping)} mappings to graph")

def apply_filter(graph: Graph, filterlist: list[URIRef]):
    """Apply predicate mappings to the graph."""
    pre_len = len(graph)
    for f_uri in filterlist:
        graph.remove((None, f_uri, None))

    logger.info(f"Filtered {pre_len - len(graph)} values from graph")

def save_graph(graph: Graph, filepath: str, format: str) -> None:
    """Save RDF graph to file."""
    graph.serialize(
        format=format,
        destination=filepath,
        encoding=ENCODING,
        auto_compact=True
    )
    logger.info("Wrote %s to %s", f"{os.path.getsize(filepath)} bytes", filepath)

# --- Main Workflow ---
def main():
    try:
        # 1. Load mapping & filter
        mapping = get_mapping_from_env()
        filterlist = get_filter_from_env()
        logger.info("Using mapping: %s", mapping)

        # 2. Load graph
        graph = load_graph(TARGET_FILEPATH, OUTPUT_FILE_FORMAT)

        # 3. Enrich data
        enrich_with_rijksmonument_data(graph)

        # 4. Apply mappings
        apply_mapping(graph, mapping)

        # 5. Apply filter
        apply_filter(graph, filterlist)

        # 6. Save graph
        save_graph(graph, TARGET_FILEPATH, OUTPUT_FILE_FORMAT)

    except BadSyntax as e:
        logger.error("RDF syntax error: %s", str(e))
    except Exception as e:
        logger.error("Unexpected error: %s", str(e))

if __name__ == "__main__":
    main()
