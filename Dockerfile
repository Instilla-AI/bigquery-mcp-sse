# Utilizza l'immagine Docker ufficiale del GenAI Toolbox come base
FROM us-central1-docker.pkg.dev/database-toolbox/toolbox/toolbox:latest

# Imposta la directory di lavoro all'interno del container
WORKDIR /app

# Copia il file tools.yaml nella directory di configurazione prevista dal toolbox
COPY tools.yaml /config/tools.yaml

# Espone la porta su cui il server MCP è in ascolto (default 5000 per genai-toolbox)
EXPOSE 5000

# Comando per avviare il server MCP, puntando al file tools.yaml e ascoltando su tutte le interfacce
CMD ["toolbox", "--tools-file", "/config/tools.yaml", "--address", "0.0.0.0"]
