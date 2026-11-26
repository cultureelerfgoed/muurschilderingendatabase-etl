"""Script that transforms and enriches linked data."""

import os
import sys
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import uritools
from rdflib import RDF, Graph, URIRef
import rdflib.namespace as ns_module
from rdflib.namespace import SDO
from rdflib.plugins.parsers.notation3 import BadSyntax

# --- Configuration ---
TARGET_FILEPATH = os.getenv('TARGET_FILEPATH', 'api-export.ttl')
BASE_URI = os.getenv('BASE_URI', 'https://muurschilderingendatabase.nl/')
OUTPUT_FILE_FORMAT = os.getenv('OUTPUT_FILE_FORMAT', 'ttl')
GRAPH_ID = os.getenv('GRAPH_ID', 'default')
ENCODING = os.getenv('ENCODING', 'utf-8')
REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '200'))
# Refuse to publish when fewer triples than this remain
MIN_TRIPLES = int(os.getenv('MIN_TRIPLES', '1'))
# Share of Rijksmonument lookups that may fail before the run is aborted
MAX_ENRICHMENT_FAILURE_RATIO = float(os.getenv('MAX_ENRICHMENT_FAILURE_RATIO', '0.1'))

RM_API_URL = 'https://api.linkeddata.cultureelerfgoed.nl/queries/rce/rest-api-rijksmonumenten/run'
CEO_RIJKSMONUMENTNUMMER =URIRef('https://linkeddata.cultureelerfgoed.nl/def/ceo#rijksmonumentnummer')
CEO_RIJKSMONUMENT = URIRef('https://linkeddata.cultureelerfgoed.nl/def/ceo#Rijksmonument')

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

def build_session() -> requests.Session:
    """Session that retries on connection errors and temporary server errors."""
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session

def enrich_with_rijksmonument_data(graph: Graph):
    """Enrich graph with Rijksmonument data."""
    # Every rijksmonumentnummer is looked up once, however often it occurs.
    # Remember which resources use a number, to report numbers that yield nothing.
    items_by_number: dict[str, set] = {}
    for subj, obj in graph.subject_objects(CEO_RIJKSMONUMENTNUMMER):
        items_by_number.setdefault(str(obj), set()).add(str(subj))
    numbers = sorted(items_by_number)
    logger.info('Looking up %i unique rijksmonumentnummers.', len(numbers))

    session = build_session()
    failures = 0
    not_found = 0
    for number in numbers:
        # Strip stray whitespace for the lookup only; the published value is untouched.
        rm_number = number.strip()
        if rm_number.startswith("RM"):
            rm_number = rm_number[2:]
        try:
            response = session.get(RM_API_URL, params={'rijksmonumentnummer': rm_number},
                                   timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            # Parse into a separate graph first, so one bad response cannot
            # leave half a result behind or break the whole enrichment.
            enrichment = Graph()
            enrichment.parse(data=response.content, format='turtle')
        except (requests.RequestException, BadSyntax) as e:
            failures += 1
            logger.error("Failed to enrich rijksmonumentnummer %s: %s", number, e)
            continue
        if (None, RDF.type, CEO_RIJKSMONUMENT) not in enrichment:
            # %r makes stray whitespace in the number visible, e.g. '517621\n'
            not_found += 1
            logger.warning("No Rijksmonument found for rijksmonumentnummer %r (%s)",
                           number, ", ".join(sorted(items_by_number[number])))
        for triple in enrichment:
            graph.add(triple)

    if numbers and failures / len(numbers) > MAX_ENRICHMENT_FAILURE_RATIO:
        raise RuntimeError(
            f"{failures} of {len(numbers)} Rijksmonument lookups failed; not publishing.")

    enr_index = 0
    for subj, pred, obj in list(graph.triples((None, CEO_RIJKSMONUMENTNUMMER, None))):
        if (subj, RDF.type, CEO_RIJKSMONUMENT) in graph:
            graph.add((subj, SDO.sameAs, obj))
            enr_index += 1

    logger.info('Enriched %i Rijksmonumenten (%i numbers without result, %i lookups failed).',
                enr_index, not_found, failures)


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
    if len(graph) < MIN_TRIPLES:
        raise RuntimeError(
            f"Only {len(graph)} triples left (minimum {MIN_TRIPLES}); not writing output.")
    save_graph(graph, TARGET_FILEPATH, OUTPUT_FILE_FORMAT)

if __name__ == "__main__":
    # Any failure must fail the job. Otherwise the untransformed (unfiltered)
    # export would still be uploaded and published in the next step.
    try:
        main()
    except BadSyntax as e:
        logger.error("RDF syntax error: %s", str(e))
        sys.exit(1)
    except Exception:  # pylint: disable=broad-except
        logger.exception("Transform failed")
        sys.exit(1)
