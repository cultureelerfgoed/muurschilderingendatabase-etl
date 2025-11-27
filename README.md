# Muurschilderingendatabase ETL

Als deel van een onderzoeksproject over muurschilderingen worden muurschilderingen en bijbehorende eigenschappen vastgelegd als linked data op een Omeka-S instantie [Muurschilderingendatabase](https://muurschilderingendatabase.nl). Om deze data te publiceren op de [Linked Data Voorziening](https://linkeddata.cultureelerfgoed.nl/) van de RCE, is deze ETL als Github Action ingericht. Github Actiopn haalt de nieuwste onderzoeksdata op via de API van de Muurschilderingendatabase, transformeert en verrijkt de data met Python en RDFLib. Vervolgens wordt de data via de Triply API gepubliceerd. 

# Installatie 

```
python -m venv .venv
source .venv/Scripts/activate
pip install -r requirements.txt
```

# How to run

```
python scripts/export_from_omeka_s.py && python scripts/transform_datamodel.py
```
