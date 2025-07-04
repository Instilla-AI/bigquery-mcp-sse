FROM python:3.11-slim

# Imposta la directory di lavoro
WORKDIR /app

# Copia i file di dipendenze
COPY requirements.txt .

# Installa le dipendenze
RUN pip install --no-cache-dir -r requirements.txt

# Copia il codice sorgente
COPY mcp_sse_server.py .

# Esponi la porta
EXPOSE 8000

# Comando per avviare l'applicazione
CMD ["python", "mcp_sse_server.py"]
