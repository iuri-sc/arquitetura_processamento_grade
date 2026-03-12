"""
MESTRE / SCHEDULER - ALUNO A

lê lista.txt (5000 linhas), divide em lotes e envia ao worker (ALUNO B) via HTTP POST com payloads JSON
"""

import argparse
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path

# configuração
BATCH_SIZE = 250  # linhas por lote
DELAY_S = 0.3  # pausa entre lotes (segundos)
INPUT_FILE = Path(__file__).parent / "lista.txt"
OUTPUT_FILE = Path(__file__).parent / "resultado_master.json"

# instruções disponíveis - variam por lote
INSTRUCTIONS = ["UPPERCASE", "BASE64", "REVERSE", "WORD_COUNT"]


# http
def post_json(url: str, payload: dict) -> tuple[int, dict]:
    # faz http post com body JSON, retorna (status_code, response_dict)
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode("utf-8") or "{}")
    except urllib.error.URLError as e:
        raise ConnectionError(str(e.reason)) from e


# lógica principal
def main(worker_url: str) -> None:
    print("MESTRE / SCHEDULER - ALUNO A")

    # lê arquivo
    if not INPUT_FILE.exists():
        print(f"Arquivo não encontrado: {INPUT_FILE}")
        print("Execute primeiro: py gerar_lista.py")
        return

    lines = INPUT_FILE.read_text(encoding="utf-8").splitlines()
    lines = [l for l in lines if l.strip()]

    print(f"Arquivo carregado: {len(lines)} linhas")
    print(f"Worker URL: {worker_url}")
    print(f"Tamanho do lote: {BATCH_SIZE} linhas")
    print(f"Delay entre lotes: {DELAY_S}s\n")

    # divide em lotes
    batches = [lines[i : i + BATCH_SIZE] for i in range(0, len(lines), BATCH_SIZE)]
    print(f"Total de lotes: {len(batches)}\n")

    # envia cada lote
    results = []
    start_time = time.time()

    for idx, batch in enumerate(batches):
        instruction = INSTRUCTIONS[idx % len(INSTRUCTIONS)]
        payload = {
            "batch_id": idx + 1,
            "total_batches": len(batches),
            "instruction": instruction,
            "data": batch,
        }

        prefix = (
            f"Lote {idx + 1:>3} / {len(batches)}",
            f"[{instruction:<10}] enviando...",
        )
        print(prefix, end="", flush=True)

        try:
            status, body = post_json(f"{worker_url}/process", payload)
            if status == 200 and body.get("success"):
                print(f"{body["processed"]} itens processados")
                results.append(
                    {
                        "batch_id": idx + 1,
                        "instruction": instruction,
                        "status": "ok",
                        "data": body["results"],
                    }
                )
            else:
                print(f"status {status}")
                results.append(
                    {
                        "batch_id": idx + 1,
                        "instruction": instruction,
                        "status": "error",
                        "detail": body,
                    }
                )
        except ConnectionError as exc:
            print(f"{exc}")
            results.append(
                {
                    "batch_id": idx + 1,
                    "instruction": instruction,
                    "status": "network_error",
                    "detail": str(exc),
                }
            )

        if idx < len(batches) - 1:
            time.sleep(DELAY_S)

    # sumário
    elapsed = time.time() - start_time
    succeeded = sum(1 for r in results if r["status"] == "ok")
    failed = len(results) - succeeded

    print(f"Lotes concluídos: {succeeded}")
    print(f"Lotes com erro: {failed}")
    print(f"Tempo real: {elapsed:.2f}s")

    # salva resultado
    OUTPUT_FILE.write_text(
        json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"Resultado salvo em: {OUTPUT_FILE}")


# entry point
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MESTRE - ENVIA LOTES AO WORKER")
    parser.add_argument(
        "--url", default=os.environ.get("WORKER_URL", "https://nonmetrically-radiopaque-elidia.ngrok-free.dev")
    )
    args = parser.parse_args()
    main(args.url)
