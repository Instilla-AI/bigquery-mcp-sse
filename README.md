# BigQuery MCP SSE Server

Server MCP (Model Context Protocol) per BigQuery con supporto SSE (Server-Sent Events) per accesso pubblico tramite API REST.

## Configurazione

### Variabili d'ambiente richieste

- `GOOGLE_CLOUD_PROJECT`: ID del progetto Google Cloud (default: `big-query-instilla`)
- `GOOGLE_APPLICATION_CREDENTIALS_JSON`: JSON completo delle credenziali del service account
- `PORT`: Porta del server (default: `8000`)

### Esempio di configurazione Railway

```
GOOGLE_CLOUD_PROJECT=big-query-instilla
GOOGLE_APPLICATION_CREDENTIALS_JSON={"type": "service_account", "project_id": "big-query-instilla", ...}
PORT=8000
```

## Endpoints disponibili

### REST API

- `GET /`: Informazioni sul server
- `GET /health`: Health check con test di connessione BigQuery
- `POST /tools`: Lista degli strumenti MCP disponibili
- `POST /execute`: Esegue uno strumento MCP

### SSE (Server-Sent Events)

- `GET /sse/tools`: Stream degli strumenti disponibili
- `POST /sse/execute`: Stream dell'esecuzione di uno strumento

## Strumenti MCP disponibili

1. **query_bigquery**: Esegue query SQL su BigQuery
2. **list_datasets**: Lista tutti i dataset nel progetto
3. **list_tables**: Lista le tabelle in un dataset
4. **describe_table**: Descrive la struttura di una tabella

## Utilizzo con n8n

Il server può essere integrato con n8n utilizzando i nodi HTTP Request per chiamare gli endpoint REST o SSE.

### Esempio di richiesta per eseguire una query

```json
POST /execute
{
  "tool_name": "query_bigquery",
  "arguments": {
    "query": "SELECT * FROM dataset.table LIMIT 10",
    "limit": 10
  }
}
```

## Deploy su Railway

1. Carica i file nel repository GitHub
2. Collega il repository a Railway
3. Configura le variabili d'ambiente
4. Deploy automatico

## Logs e debugging

Il server include logging dettagliato per:
- Inizializzazione delle credenziali
- Esecuzione delle query
- Errori di connessione
- Richieste API

Controlla i logs di Railway per debugging e monitoraggio.
