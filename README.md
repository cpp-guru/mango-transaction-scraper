# Mango transaction scraper

Collects and stores transactions made by the Mango program and Oracle program (used by the Mango program).
Does so by
* fetching new signatures from the program (getConfirmedSignaturesForAddress2)
* using those signatures to fetch transactions (getParsedConfirmedTransaction)
* parsing details of the transactions (looking at the token accounts and also transaction logs)
* stores details into a postgres database

## Run
As the app requires the postgres database as a backend - it is not self contained.
However, if the postgres db is setup:
```
yarn install
yarn start
```

## Configuration
Configuration is handled via environmental variables.
```
export TRANSACTIONS_CONNECTION_STRING=
export REQUEST_WAIT_TIME=
export ORACLE_PROGRAM_ID=
export MANGO_PROGRAM_ID=
export CLUSTER_URL=
export CLUSTER=
export TRANSACTIONS_SCRAPER_WEBHOOK_URL=
```