"""Script that downloads linked data from Omeka S API and fixes namespace issues."""

import os
import sys
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from rdflib import Graph, URIRef
import uritools

### Configuration
# Path to save file
TARGET_FILEPATH = os.getenv('TARGET_FILEPATH', 'api-export.ttl')
# URI to Omeka S endpoint
BASE_URI = os.getenv('BASE_URI', 'https://muurschilderingendatabase.nl/')
# Defines the format of the output file
OUTPUT_FILE_FORMAT = os.getenv('OUTPUT_FILE_FORMAT', 'ttl')
# Defines the graph identifier
GRAPH_ID = os.getenv('GRAPH_ID', 'default')
# Number of items per API page
PER_PAGE = int(os.getenv('PER_PAGE', '100'))
# Safety net against an endless loop; reaching it is an error, not a silent cut-off
MAX_PAGES = int(os.getenv('MAX_PAGES', '10000'))
# Timeout per request in seconds
REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '200'))
# Refuse to publish when fewer triples than this are retrieved
MIN_TRIPLES = int(os.getenv('MIN_TRIPLES', '1'))
### End of Configuration

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')


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


def fetch_items(session: requests.Session) -> Graph:
    """Retrieve all item pages from the API and parse them into one graph."""
    graph = Graph(identifier=GRAPH_ID)

    for page in range(1, MAX_PAGES + 1):
        page_url = f"{BASE_URI}api/items?format=turtle&page={page}&per_page={PER_PAGE}"
        response = session.get(page_url, timeout=REQUEST_TIMEOUT)
        # An error page must never be mistaken for "no more data".
        response.raise_for_status()

        if len(response.content) <= 2:
            logger.info("Data ended on page %s", page)
            return graph

        # Turtle is UTF-8 by definition, so parse the raw bytes.
        graph.parse(data=response.content, format="turtle")

    raise RuntimeError(f"Still receiving data after {MAX_PAGES} pages; raise MAX_PAGES.")


def is_broken(subj, obj) -> bool:
    """Triples that cannot be serialized (broken URIs from the Omeka S export)."""
    if "@context" in str(subj) or "@context" in str(obj):
        return True
    if not uritools.is_valid_uri(str(subj)):
        return True
    return isinstance(obj, URIRef) and not uritools.is_valid_uri(str(obj))


def remove_broken_triples(graph: Graph) -> None:
    """Filter out broken triples."""
    # list() because the graph is modified while looping
    for subj, pred, obj in list(graph):
        if is_broken(subj, obj):
            logger.warning("Removing an unserializable triple:")
            logger.warning("- Subject: %s", subj)
            logger.warning("- Predicate: %s", pred)
            logger.warning("- Object: %s", obj)
            graph.remove((subj, pred, obj))


def bind_namespaces(session: requests.Session, graph: Graph) -> None:
    """Retrieve namespaces from api-context endpoint and bind them."""
    response = session.get(BASE_URI + "api-context", timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    namespace_data = response.json()["@context"]

    for key in namespace_data:
        ns = URIRef(namespace_data[key].replace('\\', ''))
        logger.info("Binding namespace  %s", f"{ns} as {key}")
        graph.namespace_manager.bind(key, ns, override=True, replace=True)


def main() -> None:
    logger.info("Retrieving items from API endpoint %s", BASE_URI)
    session = build_session()

    graph = fetch_items(session)
    logger.info("Retrieved %s triples", len(graph))

    remove_broken_triples(graph)
    bind_namespaces(session, graph)

    if len(graph) < MIN_TRIPLES:
        raise RuntimeError(
            f"Only {len(graph)} triples retrieved (minimum {MIN_TRIPLES}); not writing output.")

    # Serialize graph
    logger.info("Writing  %s", f"{OUTPUT_FILE_FORMAT} file to {TARGET_FILEPATH}")
    graph.serialize(format=OUTPUT_FILE_FORMAT, destination=TARGET_FILEPATH)
    logger.info("Filesize:  %s", f"{os.path.getsize(TARGET_FILEPATH)} bytes")


if __name__ == "__main__":
    # Any failure must fail the job, otherwise broken or incomplete data would
    # be published in the next step.
    try:
        main()
    except Exception:  # pylint: disable=broad-except
        logger.exception("Export failed")
        sys.exit(1)
    finally:
        logger.info("Exiting..")
