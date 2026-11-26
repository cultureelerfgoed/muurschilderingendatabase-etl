# Muurschilderingendatabase ETL

Als deel van een onderzoeksproject over muurschilderingen worden muurschilderingen en bijbehorende eigenschappen vastgelegd als linked data op een Omeka-S instantie [Muurschilderingendatabase](https://muurschilderingendatabase.nl). Om deze data te publiceren op de [Linked Data Voorziening](https://linkeddata.cultureelerfgoed.nl/) van de RCE, is deze ETL als Github Action ingericht. De Github Action haalt de nieuwste onderzoeksdata op via de API van de Muurschilderingendatabase, transformeert en verrijkt de data met Python en RDFLib. Vervolgens wordt de data via de Triply API gepubliceerd. 

# Installatie 

```
python -m venv .venv
source .venv/Scripts/activate
pip install -r requirements.txt
```

# How to run

```
python src/export_from_omeka_s.py && python src/transform_datamodel.py
```

# Planning en keepalive

De workflow draait elke zondag om 00:00 uur (Europe/Amsterdam) en kan ook handmatig worden gestart.

GitHub schakelt geplande workflows in publieke repositories uit na 60 dagen zonder activiteit in de repository. De job `keepalive` voorkomt dat: bij elke run wordt de workflow via de GitHub-API opnieuw geactiveerd, waardoor die teller terugloopt. Dummy-commits zijn daarvoor niet nodig. De job heeft de permissie `actions: write` nodig.

# Foutafhandeling

Beide scripts stoppen met een foutcode als er iets misgaat (bron onbereikbaar, ongeldige RDF, lege uitvoer). De workflow publiceert dan niets en de vorige versie blijft op de Linked Data Voorziening staan. Tijdelijke storingen worden eerst een aantal keer opnieuw geprobeerd.

De verrijking met rijksmonumentgegevens mag voor een deel mislukken: bij meer dan 10% mislukte opvragingen stopt de run (`MAX_ENRICHMENT_FAILURE_RATIO`). Rijksmonumentnummers waarvoor niets wordt gevonden staan als waarschuwing in het log, met het bijbehorende item. Bekende oorzaken zijn een complexnummer in plaats van een rijksmonumentnummer, en rijksmonumenten zonder BAG-adres.

# Afhankelijkheden

De module `uritools` komt uit het pakket `pyrcelib` en is niet het gelijknamige pakket op PyPI. Installeer dat laatste niet apart.
