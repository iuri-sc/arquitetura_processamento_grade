"""
Gera lista.txt com 5000 linhas de exemplo
"""

words = [
    "computacao distruibuida", "processamento paralelo", "arquitetura grid", "modelo mind", "multiplas instrucoes", "multiplos dados",
    "ngrok tunnel", "servidor worker", "cliente mestre", "balanceamento de carga", "tolerancia a falhas", "escalabilidade horizontal",
    "latencia de rede", "protocolo http", "payload json", "base64 encoding", "uppercase conversion", "lote de dados",
    "processamento remoto", "comunicacao distruibuida"
]

with open("lista.txt", "w", encoding="utf-8") as f:
    for i in range(1, 5001):
        word = words[i % len(words)]
        f.write(f"linha {i:04d}: {word} - item_{i}\n")
        
print("lista.txt gerada com 5000 linhas")
