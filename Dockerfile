# Utilizza l'immagine Docker ufficiale del GenAI Toolbox come base
FROM us-central1-docker.pkg.dev/database-toolbox/toolbox/toolbox:latest

# Imposta la directory di lavoro all'interno del container
WORKDIR /app

# Espone la porta su cui il server MCP è in ascolto (default 5000 per genai-toolbox)
EXPOSE 5000

# Comando per avviare il server MCP, utilizzando la configurazione pre-costruita per BigQuery
# Il flag --address 0.0.0.0 assicura che il server sia accessibile esternamente
CMD ["toolbox", "--prebuilt", "bigquery", "--address", "0.0.0.0"]
