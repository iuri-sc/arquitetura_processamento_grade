"""
TRABALHADOR / WORKER - ALUNO B

Recebe lotes JSON do mestre, executada a instrução indicada e devolve o resultado
"""

import argparse
import base64
import json
import os
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

# processadores
def process_uppercase(lines: list[str]) -> list[str]:
    # converte cada linha para MAIÚSCULAS
    return [line.upper() for line in lines]

def process_base64(lines: list[str]) -> list[str]:
    # codifica cada linha em base64
    return [base64.b64encode(line.encode("utf-8")).decode("utf-8") for line in lines]

def process_reverse(lines: list[str]) -> list[str]:
    # inverte os caracteres de cada linha
    return [line[::-1] for line in lines]

def process_word_count(lines: list[str]) -> list[str]:
    # retorna a contagem de palavras de cada linha
    return [f"{len(line.split())} palavras: '{line}'" for line in lines]

PROCESSORS: dict = {
    "UPPERCASE": process_uppercase,
    "BASE64": process_base64,
    "REVERSE": process_reverse,
    "WORD_COUNT": process_word_count
}

# http handler
def _send_json(handler: BaseHTTPRequestHandler, status: int, payload: dict) -> None:
    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.end_headers()
    handler.wfile.write(body)
    
class WorkerHandler(BaseHTTPRequestHandler):
    # silencia o log padrão do BaseHTTPRequestHandler
    def log_message(self, format, *args):
        pass # tratado manualmente
    
    # cors
    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        
    # GET /health
    def do_GET(self):
        if self.path == "/health":
            _send_json(self, 200, {
                "status": "online",
                "worker": "aluno b",
                "timestamp": datetime.now().isoformat(),
                "instructions": list(PROCESSORS.keys())
            })
        else:
            _send_json(self, 404, {"404": "Rota não encontrada"})
            
    # POST /process
    def do_POST(self):
        if self.path != "/process":
            _send_json(self, 404, {"error": "Use POST /process"})
            return
        
        # lê body
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            _send_json(self, 400, {"success": False, "error": "JSON inválido"})
            return
        
        batch_id = payload.get("batch_id", "?")
        total_batches = payload.get("total_batches", "?")
        instruction = str(payload.get("instruction", "")).upper()
        data = payload.get("data", [])
        
        # validações
        if not instruction or not isinstance(data, list):
            _send_json(self, 422, {
                "success": False,
                "error": "Campos obrigatórios: instruction (str), data (list)"
            })
            return
        
        processor = PROCESSORS.get(instruction)
        if processor is None:
            _send_json(self, 422, {
                "success": False,
                "error": f"Instrução desconhecida: {instruction}",
                "available": list(PROCESSORS.keys()),
            })
            return
        
        # processa
        t0 = time.perf_counter()
        results = processor(data)
        elapsed = round((time.perf_counter() - t0) * 1000, 2)
        
        print(
            f"[{datetime.now().strftime("%H:%M:%S")}] "
            f"Lote {str(batch_id):>3} / {total_batches} "
            f"Instrução={instruction:<10} "
            f"itens={len(data)} ({elapsed}ms)"
        )
        
        _send_json(self, 200, {
            "success": True,
            "batch_id": batch_id,
            "instruction": instruction,
            "processed": len(results),
            "elapsed_ms": elapsed,
            "results": results
        })
        
# entry point
def main(port: int) -> None:
    print("TRABALHADOR / WORKER - ALUNO B")
    print(F"\nServidor ouvindo na porta: {port}")
    
    server = HTTPServer(("0.0.0.0", port), WorkerHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n\nWorker encerrado")
        server.server_close()
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", 3000)))
    args = parser.parse_args()
    main(args.port)