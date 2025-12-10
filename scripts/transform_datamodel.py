"""Script that transforms and enriches linked data."""

import os
import logging
import requests
from importlib import import_module
from rdflib import Graph, URIRef
import rdflib.namespace as ns_module
from rdflib.namespace import DCTERMS
from rdflib.plugins.parsers.notation3 import BadSyntax

# --- Configuration ---
TARGET_FILEPATH = os.getenv('TARGET_FILEPATH', 'data/api-export.ttl')
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
def get_mapping_from_env() -> dict:
    """Load mapping from environment variables with dynamic namespace resolution."""
    mapping = {}

    for key, value in os.environ.items():
        if key.startswith('MAP_'):
            try:
                # Parse source predicate (e.g., "DCTERMS.title")
                src_parts = key[4:].split('_')
                src_ns_name = src_parts[0]
                src_pred_name = src_parts[1].lower()
                # Parse target predicate (e.g., "SDO.name")
                tgt_ns_name, tgt_pred_name = value.split('.')

                # Dynamically import namespaces
                src_ns = getattr(ns_module, src_ns_name)
                tgt_ns = getattr(ns_module, tgt_ns_name)

                if not src_ns or not tgt_ns:
                    raise ValueError(f"Unknown namespace in {key}={value}")

                # Get predicates
                src_pred = getattr(src_ns, src_pred_name)
                tgt_pred = getattr(tgt_ns, tgt_pred_name)

                logger.info("Adding entry to transform mapping %s to %s.", f"{src_ns_name}.{src_pred_name}", f"{tgt_ns}.{tgt_pred}")
                mapping[src_pred] = tgt_pred

            except Exception as e:
                logger.warning(f"Skipping invalid mapping {key}={value}: {e}")

    return mapping

# --- Functions ---
def get_filter_from_env() -> list:
    """Load mapping from environment variables with dynamic namespace resolution."""
    filterlist = []

    for key, value in os.environ.items():
        if key.startswith('FILTER'):
            try:
                # Parse target predicate (e.g., "SDO.name")
                tgt_ns_name, tgt_pred_name = value.split('.')

                # Dynamically import namespaces
                tgt_ns = getattr(ns_module, tgt_ns_name)

                if not tgt_ns:
                    raise ValueError(f"Unknown namespace in {key}={value}")

                # Get predicates
                tgt_pred = getattr(tgt_ns, tgt_pred_name)

                logger.info("Adding entry to filterlist: %s", f"{tgt_ns}.{tgt_pred}")
                filterlist.append((tgt_ns, tgt_pred))

            except Exception as e:
                logger.warning(f"Skipping invalid filter {key}={value}: {e}")

    return filterlist

def load_graph(filepath: str, format: str) -> Graph:
    """Load RDF graph from file."""
    graph = Graph(identifier=GRAPH_ID)
    graph.parse(source=filepath, format=format)
    logger.info(f"Loaded graph with {len(graph)} triples")
    return graph

def enrich_with_rijksmonument_data(graph: Graph) -> None:
    """Enrich graph with Rijksmonument data."""
    with open("data/enrichments.ttl", "w", encoding=ENCODING) as f:
        for subj, pred, obj in graph:
            if 'Rijksmonument' in obj and graph[subj : DCTERMS.identifier]:
                for item in set(graph[subj : DCTERMS.identifier]):
                    if not isinstance(item, URIRef) and "RM" in item[0:2]:
                        RM_URI = f"https://api.linkeddata.cultureelerfgoed.nl/queries/rce/rest-api-rijksmonumenten/run?rijksmonumentnummer={item[2:]}"
                        try:
                            data = requests.get(RM_URI, timeout=200)
                            f.write(data.text)
                        except requests.RequestException as e:
                            logger.error(f"Failed to fetch {RM_URI}: {e}")
    graph.parse("data/enrichments.ttl")

def apply_mapping(graph: Graph, mapping: dict) -> None:
    """Apply predicate mappings to the graph."""
    if mapping: 
        for subj, pred, obj in list(graph):  # Use list() to avoid modification during iteration
            if pred in mapping:
                graph.remove((subj, pred, obj))
                graph.add((subj, mapping[pred], obj))
    logger.info(f"Applied {len(mapping)} mappings to graph")

def apply_filter(graph: Graph, filterlist: dict) -> None:
    """Apply predicate mappings to the graph."""
    if filterlist: 
        for subj, pred, obj in list(graph):  # Use list() to avoid modification during iteration
            if pred in filterlist:
                graph.remove((subj, pred, obj))
    logger.info(f"Filtered {len(filterlist)} predicates from graph")

def save_graph(graph: Graph, filepath: str, format: str) -> None:
    """Save RDF graph to file."""
    graph.serialize(
        format=format,
        destination=filepath,
        encoding=ENCODING,
        auto_compact=True
    )
    logger.info(f"Saved graph to {filepath} ({os.path.getsize(filepath)} bytes)")

# --- Main Workflow ---
def main():
    try:
        logger.info(f"Starting transformation for {TARGET_FILEPATH}")

        # 1. Load mapping & filter
        mapping = get_mapping_from_env()
        filterlist = get_filter_from_env()
        
        logger.info("Using mapping: %s", mapping)
        logger.info("Using filter: %s", filterlist)

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
        logger.error(f"RDF syntax error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

if __name__ == "__main__":
    main()
