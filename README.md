# BA-GetDataFromNSP

BA-GetDataFromNSP er et Python-projekt, der automatisk henter og opdaterer ticket-data fra NSP via API og lagrer dem i en SQL-database. Projektet fungerer som en lille datatjeneste, der kører kontinuerligt og leverer et stabilt datagrundlag til rapportering og analyser.

# Formål

Formålet med projektet er at automatisere dataindsamlingen fra NSP, så man undgår manuelle eksporter. Tjenesten:

- henter nye og opdaterede tickets
- normaliserer og validerer data
- opdaterer faktatabellen `tickets`
- genererer og vedligeholder dimensionstabeller
- skriver alt til SQL uden dubletter via upsert/merge

Processen kører i et simpelt loop med et tidsinterval styret af miljøvariablen `SCRIPT_RUNTIME`.

# Arkitektur og Dataflow

Hver iteration i tjenesten udfører:

1. Slå seneste `last_updated` op i databasen.
2. Hent NSP-tickets opdateret siden dette tidspunkt.
3. Rens og normalisér data (kolonner, tekstfelter, datoer).
4. Udled dimensionstabeller.
5. Skriv faktadata og dimensioner til SQL via upsert/merge.
6. Vent det antal sekunder der er angivet i `SCRIPT_RUNTIME` og kør igen.

## Projektstruktur

- main.py – styrer hele processen og loopet.
- utils/api_fetch.py – håndterer NSP-API kald.
- utils/format_df.py – formatterer og renser data.
- utils/create_ticket_df.py – mapper felter og beregner nøgletal.
- utils/create_dim_df.py – bygger dimensionstabeller.
- utils/get_engine.py – opretter SQLAlchemy-engine.
- utils/get_last_updated.py – henter seneste timestamp fra databasen.
- utils/write_to_sql.py – skriver hele datasættet til SQL.

# Miljøvariabler

Tjenesten gør brug af en række miljøvariabler, der styrer både forbindelser og adfærd. De bruges primært til:

- autentifikation mod NSP (`API_KEY`, `API_URL`)
- databaseadgang (`DB_SERVER`, `DB_NAME`, `DB_USERNAME`, `DB_PASSWORD`)
- styring af kørselsinterval (`SCRIPT_RUNTIME`)
- håndtering af tidsstempler for inkrementelle kald

De er adskilt fra selve koden for at gøre projektet mere fleksibelt og driftsvenligt – uden at følsomme oplysninger indgår direkte i repositoryet.

# Datamodel

## Faktatabel: tickets

Faktatabellen indeholder:

- én række pr. ticket
- NSP-felter mappet til snake_case
- datoer og tidsstempler
- beregnede værdier som open_days og processing_days
- relationer til dimensionstabeller

## Dimensionstabeller

Projektet genererer automatisk dimensioner baseret på felter fra NSP:

- agent_group
- task_type
- task_area
- task_status
- reason_for_rejection

# Logging

Tjenesten logger blandt andet:

- API-kald  
- formatteringstrin  
- SQL-skrivning  
- start og slut på hvert loop  

Logningen gør det muligt at følge dataflow og fejl i drift.
