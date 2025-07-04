# Aggiungi questo al tuo file mcp_sse_server.py
# Sostituisci la sezione CORS middleware con questa versione più permissiva:

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"]
)

# E modifica l'endpoint SSE con headers più specifici:
@app.get("/sse/tools")
async def sse_tools(request: Request):
    """Endpoint SSE per listare gli strumenti"""
    async def event_generator():
        try:
            if not mcp_server:
                yield f"event: error\ndata: {json.dumps({'error': 'Server MCP non inizializzato'})}\n\n"
                return
            
            # Invia evento di connessione
            yield f"event: connected\ndata: {json.dumps({'status': 'connected', 'timestamp': datetime.now().isoformat()})}\n\n"
            
            tools = await mcp_server.get_available_tools()
            # tools è già una lista di dizionari, non serve .model_dump()
            yield f"event: tools\ndata: {json.dumps({'tools': tools})}\n\n"
            
            # Mantieni la connessione viva
            while True:
                if await request.is_disconnected():
                    break
                yield f"event: heartbeat\ndata: {json.dumps({'timestamp': datetime.now().isoformat()})}\n\n"
                await asyncio.sleep(30)
                
        except Exception as e:
            logger.error(f"SSE Error: {e}")
            yield f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Requested-With",
            "Access-Control-Expose-Headers": "*",
            "Content-Type": "text/event-stream",
            "X-Accel-Buffering": "no"  # Per nginx
        }
    )
