#!/usr/bin/env python3
"""
MCP BigQuery Server con supporto HTTP Streamable
Compatible with n8n MCP Client Tool
"""

import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
from google.cloud import bigquery
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
import uvicorn

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic models for MCP protocol
class MCPRequest(BaseModel):
    jsonrpc: str = "2.0"
    id: Union[int, str]
    method: str
    params: Optional[Dict[str, Any]] = {}

class MCPResponse(BaseModel):
    jsonrpc: str = "2.0"
    id: Union[int, str]
    result: Optional[Dict[str, Any]] = None
    error: Optional[Dict[str, Any]] = None

# FastAPI app initialization
app = FastAPI(
    title="BigQuery MCP Server",
    description="Model Context Protocol server for BigQuery with HTTP Streamable support",
    version="1.0.0"
)

# CORS configuration for n8n compatibility
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global BigQuery client
bigquery_client = None

def initialize_bigquery_client():
    """Initialize BigQuery client with authentication"""
    global bigquery_client
    
    try:
        project_id = os.getenv('GOOGLE_PROJECT_ID')
        if not project_id:
            raise ValueError("GOOGLE_PROJECT_ID environment variable not set")
        
        # Try credentials JSON from environment variable
        credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
        if credentials_json:
            credentials_info = json.loads(credentials_json)
            
            # Check credential type
            if credentials_info.get('type') == 'service_account':
                # Use service account credentials
                credentials = service_account.Credentials.from_service_account_info(credentials_info)
                bigquery_client = bigquery.Client(project=project_id, credentials=credentials)
                logger.info("BigQuery client initialized with service account from JSON")
            elif credentials_info.get('type') == 'authorized_user':
                # Use authorized user credentials (OAuth2)
                from google.oauth2.credentials import Credentials
                credentials = Credentials(
                    token=None,  # Will be refreshed automatically
                    refresh_token=credentials_info.get('refresh_token'),
                    client_id=credentials_info.get('client_id'),
                    client_secret=credentials_info.get('client_secret'),
                    token_uri='https://oauth2.googleapis.com/token'
                )
                bigquery_client = bigquery.Client(project=project_id, credentials=credentials)
                logger.info("BigQuery client initialized with authorized user credentials from JSON")
            else:
                raise ValueError(f"Unsupported credential type: {credentials_info.get('type')}")
        else:
            # Fall back to Application Default Credentials
            bigquery_client = bigquery.Client(project=project_id)
            logger.info("BigQuery client initialized with Application Default Credentials")
            
    except Exception as e:
        logger.error(f"Failed to initialize BigQuery client: {e}")
        raise

# MCP Tools definitions
MCP_TOOLS = [
    {
        "name": "query_bigquery",
        "description": "Esegue una query SQL su BigQuery e restituisce i risultati in formato strutturato",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Query SQL da eseguire su BigQuery (SELECT, WITH, etc.)"
                },
                "limit": {
                    "type": "integer",
                    "description": "Numero massimo di righe da restituire (default: 100, max: 1000)",
                    "default": 100,
                    "minimum": 1,
                    "maximum": 1000
                },
                "dry_run": {
                    "type": "boolean",
                    "description": "Se true, valida la query senza eseguirla (default: false)",
                    "default": False
                }
            },
            "required": ["query"]
        }
    },
    {
        "name": "list_datasets",
        "description": "Lista tutti i dataset disponibili nel progetto BigQuery corrente",
        "inputSchema": {
            "type": "object",
            "properties": {
                "include_hidden": {
                    "type": "boolean",
                    "description": "Include dataset nascosti (default: false)",
                    "default": False
                }
            }
        }
    },
    {
        "name": "list_tables",
        "description": "Lista tutte le tabelle e viste in un dataset specificato",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dataset_id": {
                    "type": "string",
                    "description": "ID del dataset di cui listare le tabelle"
                },
                "include_views": {
                    "type": "boolean",
                    "description": "Include le viste oltre alle tabelle (default: true)",
                    "default": True
                }
            },
            "required": ["dataset_id"]
        }
    },
    {
        "name": "describe_table",
        "description": "Ottieni informazioni dettagliate su schema e metadati di una tabella",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dataset_id": {
                    "type": "string",
                    "description": "ID del dataset contenente la tabella"
                },
                "table_id": {
                    "type": "string",
                    "description": "ID della tabella da descrivere"
                },
                "include_schema": {
                    "type": "boolean",
                    "description": "Include schema dettagliato dei campi (default: true)",
                    "default": True
                }
            },
            "required": ["dataset_id", "table_id"]
        }
    },
    {
        "name": "get_sample_data",
        "description": "Ottieni dati di esempio da una tabella per comprenderne la struttura",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dataset_id": {
                    "type": "string",
                    "description": "ID del dataset"
                },
                "table_id": {
                    "type": "string",
                    "description": "ID della tabella"
                },
                "limit": {
                    "type": "integer",
                    "description": "Numero di righe di esempio (default: 5, max: 50)",
                    "default": 5,
                    "minimum": 1,
                    "maximum": 50
                }
            },
            "required": ["dataset_id", "table_id"]
        }
    }
]

async def execute_tool(tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a specific MCP tool"""
    
    if not bigquery_client:
        return {
            "success": False,
            "error": "BigQuery client not initialized"
        }
    
    try:
        if tool_name == "query_bigquery":
            return await query_bigquery(
                arguments.get("query"),
                arguments.get("limit", 100),
                arguments.get("dry_run", False)
            )
        elif tool_name == "list_datasets":
            return await list_datasets(arguments.get("include_hidden", False))
        elif tool_name == "list_tables":
            return await list_tables(
                arguments.get("dataset_id"),
                arguments.get("include_views", True)
            )
        elif tool_name == "describe_table":
            return await describe_table(
                arguments.get("dataset_id"),
                arguments.get("table_id"),
                arguments.get("include_schema", True)
            )
        elif tool_name == "get_sample_data":
            return await get_sample_data(
                arguments.get("dataset_id"),
                arguments.get("table_id"),
                arguments.get("limit", 5)
            )
        else:
            return {
                "success": False,
                "error": f"Tool sconosciuto: {tool_name}"
            }
    except Exception as e:
        logger.error(f"Error executing tool {tool_name}: {e}")
        return {
            "success": False,
            "error": str(e)
        }

async def query_bigquery(query: str, limit: int = 100, dry_run: bool = False) -> Dict[str, Any]:
    """Execute BigQuery SQL query"""
    try:
        job_config = bigquery.QueryJobConfig()
        job_config.dry_run = dry_run
        job_config.use_query_cache = True
        job_config.maximum_bytes_billed = 1000000000  # 1GB limit
        
        # Execute query
        query_job = bigquery_client.query(query, job_config=job_config)
        
        if dry_run:
            return {
                "success": True,
                "dry_run": True,
                "bytes_processed": query_job.total_bytes_processed,
                "query_valid": True
            }
        
        # Get results
        results = query_job.result(max_results=limit)
        
        # Convert to list of dictionaries
        rows = []
        for row in results:
            row_dict = {}
            for i, field in enumerate(results.schema):
                value = row[i]
                # Handle datetime and other special types
                if isinstance(value, datetime):
                    value = value.isoformat()
                elif hasattr(value, 'isoformat'):
                    value = value.isoformat()
                row_dict[field.name] = value
            rows.append(row_dict)
        
        return {
            "success": True,
            "row_count": len(rows),
            "total_bytes_processed": query_job.total_bytes_processed,
            "total_bytes_billed": query_job.total_bytes_billed,
            "cache_hit": query_job.cache_hit,
            "schema": [{"name": field.name, "type": field.field_type, "mode": field.mode} 
                      for field in results.schema],
            "data": rows
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

async def list_datasets(include_hidden: bool = False) -> Dict[str, Any]:
    """List all datasets in the project"""
    try:
        datasets = bigquery_client.list_datasets(include_all=include_hidden)
        
        dataset_list = []
        for dataset in datasets:
            dataset_ref = bigquery_client.get_dataset(dataset.dataset_id)
            dataset_list.append({
                "dataset_id": dataset.dataset_id,
                "friendly_name": dataset_ref.friendly_name,
                "description": dataset_ref.description,
                "location": dataset_ref.location,
                "created": dataset_ref.created.isoformat() if dataset_ref.created else None,
                "modified": dataset_ref.modified.isoformat() if dataset_ref.modified else None,
                "labels": dict(dataset_ref.labels) if dataset_ref.labels else {}
            })
        
        return {
            "success": True,
            "dataset_count": len(dataset_list),
            "datasets": dataset_list
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

async def list_tables(dataset_id: str, include_views: bool = True) -> Dict[str, Any]:
    """List tables in a dataset"""
    try:
        dataset = bigquery_client.dataset(dataset_id)
        tables = bigquery_client.list_tables(dataset)
        
        table_list = []
        for table in tables:
            table_ref = bigquery_client.get_table(table)
            
            # Filter views if requested
            if not include_views and table_ref.table_type == "VIEW":
                continue
                
            table_list.append({
                "table_id": table.table_id,
                "table_type": table_ref.table_type,
                "num_rows": table_ref.num_rows,
                "num_bytes": table_ref.num_bytes,
                "created": table_ref.created.isoformat() if table_ref.created else None,
                "modified": table_ref.modified.isoformat() if table_ref.modified else None,
                "description": table_ref.description,
                "labels": dict(table_ref.labels) if table_ref.labels else {}
            })
        
        return {
            "success": True,
            "table_count": len(table_list),
            "tables": table_list
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

async def describe_table(dataset_id: str, table_id: str, include_schema: bool = True) -> Dict[str, Any]:
    """Get detailed table information"""
    try:
        table = bigquery_client.get_table(f"{dataset_id}.{table_id}")
        
        result = {
            "success": True,
            "table_id": table.table_id,
            "dataset_id": table.dataset_id,
            "project_id": table.project,
            "table_type": table.table_type,
            "num_rows": table.num_rows,
            "num_bytes": table.num_bytes,
            "created": table.created.isoformat() if table.created else None,
            "modified": table.modified.isoformat() if table.modified else None,
            "expires": table.expires.isoformat() if table.expires else None,
            "description": table.description,
            "location": table.location,
            "labels": dict(table.labels) if table.labels else {}
        }
        
        if include_schema and table.schema:
            result["schema"] = [
                {
                    "name": field.name,
                    "type": field.field_type,
                    "mode": field.mode,
                    "description": field.description
                }
                for field in table.schema
            ]
        
        return result
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

async def get_sample_data(dataset_id: str, table_id: str, limit: int = 5) -> Dict[str, Any]:
    """Get sample data from table"""
    try:
        query = f"SELECT * FROM `{dataset_id}.{table_id}` LIMIT {limit}"
        return await query_bigquery(query, limit)
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

async def handle_mcp_request(request_data: Dict[str, Any]) -> Dict[str, Any]:
    """Handle MCP protocol requests"""
    method = request_data.get("method")
    params = request_data.get("params", {})
    request_id = request_data.get("id")
    
    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": {}
                },
                "serverInfo": {
                    "name": "bigquery-mcp-server",
                    "version": "1.0.0",
                    "description": "BigQuery MCP Server with HTTP Streamable support"
                }
            }
        }
    
    elif method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "tools": MCP_TOOLS
            }
        }
    
    elif method == "tools/call":
        tool_name = params.get("name")
        arguments = params.get("arguments", {})
        
        tool_result = await execute_tool(tool_name, arguments)
        
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(tool_result, indent=2, ensure_ascii=False)
                    }
                ]
            }
        }
    
    else:
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": -32601,
                "message": f"Method not found: {method}"
            }
        }

# HTTP Routes

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test BigQuery connection
        if bigquery_client:
            # Simple test query
            job = bigquery_client.query("SELECT 1 as test", job_config=bigquery.QueryJobConfig(dry_run=True))
            bq_status = "connected"
        else:
            bq_status = "not_initialized"
    except Exception as e:
        bq_status = f"error: {str(e)}"
    
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "bigquery_status": bq_status,
        "version": "1.0.0"
    }

@app.get("/tools")
async def list_tools():
    """List available tools (for debugging)"""
    return {"tools": MCP_TOOLS}

@app.post("/stream")
async def mcp_stream_handler(request: Request):
    """HTTP Streamable endpoint for MCP protocol"""
    try:
        request_data = await request.json()
        logger.info(f"MCP Request: {request_data.get('method')} (ID: {request_data.get('id')})")
        
        response_data = await handle_mcp_request(request_data)
        
        # For HTTP Streamable, we return the response as text with newline
        response_text = json.dumps(response_data, ensure_ascii=False) + "\n"
        
        return StreamingResponse(
            iter([response_text]),
            media_type="text/plain",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive"
            }
        )
        
    except Exception as e:
        logger.error(f"Error handling MCP request: {e}")
        error_response = {
            "jsonrpc": "2.0",
            "id": request_data.get("id") if 'request_data' in locals() else None,
            "error": {
                "code": -32603,
                "message": str(e)
            }
        }
        error_text = json.dumps(error_response) + "\n"
        return StreamingResponse(
            iter([error_text]),
            media_type="text/plain",
            status_code=500
        )

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    logger.info("🚀 Starting BigQuery MCP Server...")
    initialize_bigquery_client()
    logger.info("✅ BigQuery MCP Server ready!")

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(
        "mcp_sse_server:app",
        host="0.0.0.0",
        port=port,
        log_level="info",
        access_log=True
    )
