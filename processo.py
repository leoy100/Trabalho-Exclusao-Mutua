"""Processo cliente do algoritmo centralizado de exclusão mútua."""

import argparse
from datetime import datetime
import socket
import sys
import time

from protocolo import GRANT, REQUEST, RELEASE, ErroProtocolo, enviar_mensagem, receber_mensagem


def executar(identificador, repeticoes, espera, host, porta, arquivo_resultado):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as cliente:
        cliente.connect((host, porta))

        for rodada in range(1, repeticoes + 1):
            enviar_mensagem(cliente, REQUEST, identificador)

            mensagem = receber_mensagem(cliente)
            if mensagem is None:
                raise ConnectionError("Coordenador encerrou a conexão antes de enviar GRANT.")

            tipo, pid = mensagem
            if tipo != GRANT or pid != identificador:
                raise ErroProtocolo(
                    f"Esperava GRANT para o processo {identificador}; recebeu tipo={tipo}, processo={pid}."
                )

            # A escrita e a espera fazem parte da região crítica. RELEASE só é enviado depois.
            instante = datetime.now().astimezone().isoformat(timespec="milliseconds")
            with open(arquivo_resultado, "a", encoding="utf-8", newline="") as resultado:
                resultado.write(f"{identificador};{instante}\n")

            time.sleep(espera)
            enviar_mensagem(cliente, RELEASE, identificador)

        print(f"Processo {identificador} concluiu {repeticoes} repetição(ões).", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Processo cliente de exclusão mútua distribuída")
    parser.add_argument("--id", type=int, required=True, dest="identificador")
    parser.add_argument("--repetitions", type=int, required=True)
    parser.add_argument("--k", type=float, required=True, help="espera em segundos dentro da região crítica")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=50000)
    parser.add_argument("--result", default="resultado.txt")
    args = parser.parse_args()

    if args.identificador < 1:
        parser.error("--id deve ser um inteiro positivo")
    if args.repetitions < 1:
        parser.error("--repetitions deve ser pelo menos 1")
    if args.k < 0:
        parser.error("--k não pode ser negativo")
    if args.port < 1 or args.port > 65535:
        parser.error("--port deve estar entre 1 e 65535")

    try:
        executar(args.identificador, args.repetitions, args.k, args.host, args.port, args.result)
    except (OSError, ConnectionError, ErroProtocolo) as erro:
        print(f"Processo {args.identificador}: {erro}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
