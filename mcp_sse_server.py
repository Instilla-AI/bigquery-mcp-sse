#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import tempfile
from typing import Any, Dict, List, Optional

from google.cloud import bigquery
from google.oauth2 import service_account
import google.auth.exceptions

from mcp.server import Server
from mcp.types import Tool, TextContent
from sse_starlette import EventSourceResponse
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Configurazione logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BigQueryMCPServer:
    def __init__(self):
        self.server = Server("bigquery-mcp")
        self.client = None
        self.project_id = None
        self._initialize_client()
        self._setup_tools()

    def _initialize_client(self):
        """Inizializza il client BigQuery con gestione delle credenziali"""
        try:
            # Ottieni project ID dalle variabili d'ambiente
            self.project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'big-query-instilla')
            
            # Gestione delle credenziali
            credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
            
            if credentials_json:
                logger.info("Utilizzando credenziali da GOOGLE_APPLICATION_CREDENTIALS_JSON")
                # Parse del JSON delle credenziali
                try:
                    credentials_info = json.loads(credentials_json)
                    logger.info(f"Tipo di credenziali: {credentials_info.get('type', 'unknown')}")
                    
                    # Gestione di diversi tipi di credenziali
                    if credentials_info.get('type') == 'service_account':
                        credentials = service_account.Credentials.from_service_account_info(
                            credentials_info,
                            scopes=['https://www.googleapis.com/auth/bigquery']
                        )
                    elif credentials_info.get('type') == 'authorized_user':
                        # Per credenziali authorized_user, creiamo un file temporaneo
                        import tempfile
                        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                            json.dump(credentials_info, f)
                            temp_file = f.name
                        
                        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = temp_file
                        credentials = None  # Usa le credenziali di default dal file
                    else:
                        # Prova con le credenziali come oggetto generico
                        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                            json.dump(credentials_info, f)
                            temp_file = f.name
                        
                        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = temp_file
                        credentials = None
                    
                    self.client = bigquery.Client(
                        project=self.project_id, 
                        credentials=credentials
                    )
                    logger.info(f"Client BigQuery inizializzato con successo per il progetto: {self.project_id}")
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Errore nel parsing del JSON delle credenziali: {e}")
                    raise
                except Exception as e:
                    logger.error(f"Errore nella configurazione delle credenziali: {e}")
                    # Fallback: prova senza credenziali specifiche
                    logger.info("Tentativo fallback con credenziali di default")
                    self.client = bigquery.Client(project=self.project_id)
            else:
                # Fallback alle credenziali di default
                logger.info("Tentativo di utilizzo delle credenziali di default")
                self.client = bigquery.Client(project=self.project_id)
                logger.info(f"Client BigQuery inizializzato con credenziali di default per il progetto: {self.project_id}")
                
        except Exception as e:
            logger.error(f"Errore nell'inizializzazione del client BigQuery: {e}")
            raise

    def _setup_tools(self):
        """Configura gli strumenti MCP"""
        
        @self.server.list_tools()
        async def list_tools() -> List[Tool]:
            return [
                Tool(
                    name="query_bigquery",
                    description="Esegue una query SQL su BigQuery",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "Query SQL da eseguire"
                            },
                            "limit": {
                                "type": "number",
                                "description": "Limite di righe da restituire (default: 100)",
                                "default": 100
                            }
                        },
                        "required": ["query"]
                    }
                ),
                Tool(
                    name="list_datasets",
                    description="Lista tutti i dataset disponibili nel progetto",
                    inputSchema={
                        "type": "object",
                        "properties": {}
                    }
                ),
                Tool(
                    name="list_tables", 
                    description="Lista le tabelle in un dataset",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "dataset_id": {
                                "type": "string",
                                "description": "ID del dataset"
                            }
                        },
                        "required": ["dataset_id"]
                    }
                ),
                Tool(
                    name="describe_table",
                    description="Descrive la struttura di una tabella",
                    inputSchema={
                        "type": "object", 
                        "properties": {
                            "dataset_id": {
                                "type": "string",
                                "description": "ID del dataset"
                            },
                            "table_id": {
                                "type": "string", 
                                "description": "ID della tabella"
                            }
                        },
                        "required": ["dataset_id", "table_id"]
                    }
                )
            ]

        @self.server.call_tool()
        async def call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
            try:
                if name == "query_bigquery":
                    return await self._execute_query(arguments)
                elif name == "list_datasets":
                    return await self._list_datasets()
                elif name == "list_tables":
                    return await self._list_tables(arguments)
                elif name == "describe_table":
                    return await self._describe_table(arguments)
                else:
                    return [TextContent(type="text", text=f"Strumento sconosciuto: {name}")]
            except Exception as e:
                logger.error(f"Errore nell'esecuzione dello strumento {name}: {e}")
                return [TextContent(type="text", text=f"Errore: {str(e)}")]

    async def _execute_query(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """Esegue una query BigQuery"""
        query = arguments.get("query")
        limit = arguments.get("limit", 100)
        
        if not query:
            return [TextContent(type="text", text="Query non fornita")]
        
        try:
            # Aggiungi LIMIT se non presente
            if "LIMIT" not in query.upper() and limit:
                query = f"{query} LIMIT {limit}"
            
            logger.info(f"Esecuzione query: {query}")
            query_job = self.client.query(query)
            results = query_job.result()
            
            # Converti i risultati in formato leggibile
            rows = []
            for row in results:
                rows.append(dict(row))
            
            if not rows:
                return [TextContent(type="text", text="Nessun risultato trovato")]
            
            # Formatta la risposta
            response = {
                "query": query,
                "total_rows": len(rows),
                "results": rows
            }
            
            return [TextContent(type="text", text=json.dumps(response, indent=2, default=str))]
            
        except Exception as e:
            logger.error(f"Errore nell'esecuzione della query: {e}")
            return [TextContent(type="text", text=f"Errore nella query: {str(e)}")]

    async def _list_datasets(self) -> List[TextContent]:
        """Lista tutti i dataset nel progetto"""
        try:
            datasets = list(self.client.list_datasets())
            
            if not datasets:
                return [TextContent(type="text", text="Nessun dataset trovato")]
            
            dataset_list = []
            for dataset in datasets:
                dataset_info = {
                    "dataset_id": dataset.dataset_id,
                    "project": dataset.project,
                    "full_dataset_id": f"{dataset.project}.{dataset.dataset_id}"
                }
                dataset_list.append(dataset_info)
            
            response = {
                "project_id": self.project_id,
                "total_datasets": len(dataset_list),
                "datasets": dataset_list
            }
            
            return [TextContent(type="text", text=json.dumps(response, indent=2))]
            
        except Exception as e:
            logger.error(f"Errore nel recuperare i dataset: {e}")
            return [TextContent(type="text", text=f"Errore: {str(e)}")]

    async def _list_tables(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """Lista le tabelle in un dataset"""
        dataset_id = arguments.get("dataset_id")
        
        if not dataset_id:
            return [TextContent(type="text", text="dataset_id non fornito")]
        
        try:
            dataset_ref = self.client.dataset(dataset_id)
            tables = list(self.client.list_tables(dataset_ref))
            
            if not tables:
                return [TextContent(type="text", text=f"Nessuna tabella trovata nel dataset {dataset_id}")]
            
            table_list = []
            for table in tables:
                table_info = {
                    "table_id": table.table_id,
                    "dataset_id": table.dataset_id,
                    "project": table.project,
                    "full_table_id": f"{table.project}.{table.dataset_id}.{table.table_id}",
                    "table_type": table.table_type
                }
                table_list.append(table_info)
            
            response = {
                "dataset_id": dataset_id,
                "total_tables": len(table_list),
                "tables": table_list
            }
            
            return [TextContent(type="text", text=json.dumps(response, indent=2))]
            
        except Exception as e:
            logger.error(f"Errore nel recuperare le tabelle: {e}")
            return [TextContent(type="text", text=f"Errore: {str(e)}")]

    async def _describe_table(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """Descrive la struttura di una tabella"""
        dataset_id = arguments.get("dataset_id")
        table_id = arguments.get("table_id")
        
        if not dataset_id or not table_id:
            return [TextContent(type="text", text="dataset_id e table_id sono obbligatori")]
        
        try:
            table_ref = self.client.dataset(dataset_id).table(table_id)
            table = self.client.get_table(table_ref)
            
            fields = []
            for field in table.schema:
                field_info = {
                    "name": field.name,
                    "field_type": field.field_type,
                    "mode": field.mode,
                    "description": field.description
                }
                fields.append(field_info)
            
            response = {
                "project": table.project,
                "dataset_id": table.dataset_id,
                "table_id": table.table_id,
                "full_table_id": f"{table.project}.{table.dataset_id}.{table.table_id}",
                "num_rows": table.num_rows,
                "num_bytes": table.num_bytes,
                "created": table.created.isoformat() if table.created else None,
                "modified": table.modified.isoformat() if table.modified else None,
                "schema": fields
            }
            
            return [TextContent(type="text", text=json.dumps(response, indent=2, default=str))]
            
        except Exception as e:
            logger.error(f"Errore nel descrivere la tabella: {e}")
            return [TextContent(type="text", text=f"Errore: {str(e)}")]

    async def get_available_tools(self):
        """Metodo helper per ottenere la lista degli strumenti"""
        try:
            # Chiamiamo direttamente la funzione registrata
            tools_func = self.server._tools_handler
            if tools_func:
                return await tools_func()
            else:
                return []
        except Exception as e:
            logger.error(f"Errore nel recuperare gli strumenti: {e}")
            return []

    async def execute_tool(self, name: str, arguments: Dict[str, Any]):
        """Metodo helper per eseguire uno strumento"""
        try:
            # Chiamiamo direttamente la funzione registrata
            call_func = self.server._call_tool_handler
            if call_func:
                return await call_func(name, arguments)
            else:
                return [TextContent(type="text", text="Nessun gestore strumenti configurato")]
        except Exception as e:
            logger.error(f"Errore nell'esecuzione dello strumento: {e}")
            return [TextContent(type="text", text=f"Errore: {str(e)}")]

# FastAPI app per SSE
app = FastAPI(title="BigQuery MCP SSE Server")

# Configura CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Istanza globale del server MCP
mcp_server = None

@app.on_event("startup")
async def startup_event():
    global mcp_server
    try:
        mcp_server = BigQueryMCPServer()
        logger.info("Server MCP BigQuery inizializzato con successo")
    except Exception as e:
        logger.error(f"Errore nell'inizializzazione del server MCP: {e}")
        raise

@app.get("/")
async def root():
    return {
        "message": "BigQuery MCP SSE Server",
        "status": "running",
        "project_id": mcp_server.project_id if mcp_server else None
    }

@app.get("/health")
async def health_check():
    try:
        if mcp_server and mcp_server.client:
            # Test connessione con una query semplice
            query = "SELECT 1 as test_connection LIMIT 1"
            query_job = mcp_server.client.query(query)
            result = query_job.result()
            return {
                "status": "healthy",
                "bigquery_connection": "ok",
                "project_id": mcp_server.project_id
            }
        else:
            return {
                "status": "unhealthy", 
                "bigquery_connection": "failed",
                "error": "MCP server not initialized"
            }
    except Exception as e:
        return {
            "status": "unhealthy",
            "bigquery_connection": "failed", 
            "error": str(e)
        }

@app.post("/tools")
async def list_tools():
    """Endpoint per listare gli strumenti disponibili"""
    try:
        if not mcp_server:
            return {"error": "Server MCP non inizializzato"}
        
        tools = await mcp_server.get_available_tools()
        return {"tools": [tool.model_dump() for tool in tools]}
    except Exception as e:
        return {"error": str(e)}

@app.post("/execute")
async def execute_tool(request: dict):
    """Endpoint per eseguire uno strumento"""
    try:
        if not mcp_server:
            return {"error": "Server MCP non inizializzato"}
        
        tool_name = request.get("tool_name")
        arguments = request.get("arguments", {})
        
        if not tool_name:
            return {"error": "tool_name richiesto"}
        
        result = await mcp_server.execute_tool(tool_name, arguments)
        return {"result": [content.model_dump() for content in result]}
    except Exception as e:
        return {"error": str(e)}

@app.get("/sse/tools")
async def sse_tools(request: Request):
    """Endpoint SSE per listare gli strumenti"""
    async def event_generator():
        try:
            if not mcp_server:
                yield {
                    "event": "error",
                    "data": json.dumps({"error": "Server MCP non inizializzato"})
                }
                return
            
            tools = await mcp_server.get_available_tools()
            yield {
                "event": "tools",
                "data": json.dumps({"tools": [tool.model_dump() for tool in tools]})
            }
        except Exception as e:
            yield {
                "event": "error", 
                "data": json.dumps({"error": str(e)})
            }
    
    return EventSourceResponse(event_generator())

@app.post("/sse/execute")
async def sse_execute(request: Request):
    """Endpoint SSE per eseguire strumenti"""
    try:
        body = await request.json()
    except:
        body = {}
    
    async def event_generator():
        try:
            if not mcp_server:
                yield {
                    "event": "error",
                    "data": json.dumps({"error": "Server MCP non inizializzato"})
                }
                return
            
            tool_name = body.get("tool_name")
            arguments = body.get("arguments", {})
            
            if not tool_name:
                yield {
                    "event": "error",
                    "data": json.dumps({"error": "tool_name richiesto"})
                }
                return
            
            # Invia evento di inizio
            yield {
                "event": "start",
                "data": json.dumps({"tool_name": tool_name, "arguments": arguments})
            }
            
            # Esegui lo strumento
            result = await mcp_server.execute_tool(tool_name, arguments)
            
            # Invia il risultato
            yield {
                "event": "result",
                "data": json.dumps({"result": [content.model_dump() for content in result]})
            }
            
            # Invia evento di completamento
            yield {
                "event": "complete",
                "data": json.dumps({"status": "success"})
            }
            
        except Exception as e:
            yield {
                "event": "error",
                "data": json.dumps({"error": str(e)})
            }
    
    return EventSourceResponse(event_generator())

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
