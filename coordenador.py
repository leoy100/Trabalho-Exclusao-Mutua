"""Coordenador do algoritmo centralizado de exclusão mútua."""

import argparse
import csv
from collections import deque
from datetime import datetime
import socket
import sys
import threading

from protocolo import (
    GRANT,
    NOMES,
    RELEASE,
    REQUEST,
    ErroProtocolo,
    enviar_mensagem,
    receber_mensagem,
)


class Coordenador:
    def __init__(self, host, porta, caminho_log):
        self.host = host
        self.porta = porta
        self.caminho_log = caminho_log

        # Q segue o pseudocódigo da aula: Q[0] está na região crítica;
        # os elementos seguintes aguardam. Não há um dono separado.
        self.Q = deque()
        self.sockets = {}
        self.atendimentos = {}
        self.lock = threading.Lock()
        self.encerrar = threading.Event()
        self.sequencia_log = 0
        self.arquivo_log = None
        self.csv_log = None

    def registrar(self, direcao, tipo, origem, destino, pid="", rodada="", detalhe=""):
        """Chamado sob lock para manter estado e ordem do log consistentes."""
        self.sequencia_log += 1
        instante = datetime.now().astimezone().isoformat(timespec="milliseconds")
        nome_tipo = NOMES.get(tipo, tipo)
        self.csv_log.writerow([
            self.sequencia_log, instante, direcao, nome_tipo,
            origem, destino, pid, rodada, detalhe,
        ])
        self.arquivo_log.flush()

    def registrar_erro(self, detalhe, pid="", rodada=""):
        with self.lock:
            self.registrar("ERRO", "ERRO", "processo", "coordenador", pid, rodada, detalhe)
        print(f"[coordenador] Erro: {detalhe}", flush=True)

    def conceder(self, pedido):
        """Concede o acesso ao primeiro elemento de Q. Chamado sob lock."""
        pid, rodada, conexao = pedido
        try:
            enviar_mensagem(conexao, GRANT, pid)
        except OSError as erro:
            self.registrar("ERRO", "ERRO", "coordenador", f"processo {pid}", pid, rodada,
                           f"falha ao enviar GRANT: {erro}")
            return

        self.atendimentos[pid] = self.atendimentos.get(pid, 0) + 1
        self.registrar("TX", GRANT, "coordenador", f"processo {pid}", pid, rodada)

    def tratar_cliente(self, conexao, endereco):
        pid_conexao = None
        rodada = 0

        try:
            while not self.encerrar.is_set():
                try:
                    mensagem = receber_mensagem(conexao)
                except socket.timeout:
                    continue
                except (OSError, ErroProtocolo) as erro:
                    self.registrar_erro(f"mensagem inválida de {endereco}: {erro}", pid_conexao or "")
                    break

                if mensagem is None:
                    break

                tipo, pid = mensagem
                fechar_conexao = False

                with self.lock:
                    if pid_conexao is None:
                        if tipo != REQUEST:
                            self.registrar("ERRO", "ERRO", f"processo {pid}", "coordenador", pid, "",
                                           "a primeira mensagem da conexão deve ser REQUEST")
                            fechar_conexao = True
                        elif pid in self.sockets:
                            self.registrar("ERRO", "ERRO", f"processo {pid}", "coordenador", pid, "",
                                           "identificador já está conectado")
                            fechar_conexao = True
                        else:
                            pid_conexao = pid
                            self.sockets[pid] = conexao
                            self.atendimentos.setdefault(pid, 0)

                    if fechar_conexao:
                        pass
                    elif pid != pid_conexao:
                        self.registrar("ERRO", "ERRO", f"processo {pid}", "coordenador", pid, rodada,
                                       "o identificador mudou durante a conexão")
                    elif tipo == REQUEST:
                        rodada += 1
                        self.registrar("RX", REQUEST, f"processo {pid}", "coordenador", pid, rodada)

                        if any(pedido[0] == pid for pedido in self.Q):
                            self.registrar("ERRO", "ERRO", f"processo {pid}", "coordenador", pid, rodada,
                                           "o processo já tem um pedido na fila")
                        else:
                            fila_estava_vazia = not self.Q
                            pedido = (pid, rodada, conexao)
                            self.Q.append(pedido)
                            if fila_estava_vazia:
                                self.conceder(self.Q[0])

                    elif tipo == RELEASE:
                        self.registrar("RX", RELEASE, f"processo {pid}", "coordenador", pid, rodada)

                        if not self.Q:
                            self.registrar("ERRO", "ERRO", f"processo {pid}", "coordenador", pid, rodada,
                                           "RELEASE recebido com fila vazia")
                        elif self.Q[0][0] != pid or self.Q[0][1] != rodada:
                            self.registrar("ERRO", "ERRO", f"processo {pid}", "coordenador", pid, rodada,
                                           "RELEASE não corresponde ao primeiro pedido da fila")
                        else:
                            self.Q.popleft()
                            if self.Q:
                                self.conceder(self.Q[0])

                    else:
                        # GRANT é uma mensagem enviada pelo coordenador, nunca por um cliente.
                        self.registrar("ERRO", "ERRO", f"processo {pid}", "coordenador", pid, rodada,
                                       "cliente enviou um tipo de mensagem inesperado")

                if fechar_conexao:
                    break

        finally:
            with self.lock:
                if pid_conexao is not None:
                    if any(pedido[0] == pid_conexao for pedido in self.Q):
                        self.registrar("ERRO", "ERRO", f"processo {pid_conexao}", "coordenador",
                                       pid_conexao, rodada, "conexão encerrada enquanto havia pedido na fila")
                    if self.sockets.get(pid_conexao) is conexao:
                        del self.sockets[pid_conexao]
                    self.registrar("INFO", "DESCONECTADO", f"processo {pid_conexao}", "coordenador",
                                   pid_conexao, rodada)
            try:
                conexao.close()
            except OSError:
                pass

    def interface(self):
        print("Comandos: 1 = fila | 2 = atendimentos | 3 = encerrar", flush=True)
        while not self.encerrar.is_set():
            try:
                comando = input("Comando: ").strip()
            except EOFError:
                self.encerrar.set()
                break

            if comando == "1":
                with self.lock:
                    if not self.Q:
                        print("Fila vazia.", flush=True)
                    else:
                        for indice, (pid, rodada, _) in enumerate(self.Q):
                            estado = "na região crítica" if indice == 0 else "aguardando"
                            print(f"{indice + 1}. processo {pid}, repetição {rodada} ({estado})", flush=True)
            elif comando == "2":
                with self.lock:
                    if not self.atendimentos:
                        print("Nenhum processo atendido.", flush=True)
                    else:
                        for pid in sorted(self.atendimentos):
                            print(f"Processo {pid}: {self.atendimentos[pid]} atendimento(s)", flush=True)
            elif comando == "3":
                print("Encerrando coordenador.", flush=True)
                self.encerrar.set()
            elif comando:
                print("Comando inválido. Use 1, 2 ou 3.", flush=True)

    def executar(self):
        self.arquivo_log = open(self.caminho_log, "w", newline="", encoding="utf-8")
        self.csv_log = csv.writer(self.arquivo_log, delimiter=";")
        self.csv_log.writerow([
            "ordem", "timestamp", "direcao", "tipo", "origem", "destino",
            "processo", "repeticao", "detalhe",
        ])
        self.arquivo_log.flush()

        servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            servidor.bind((self.host, self.porta))
            servidor.listen()
            servidor.settimeout(0.5)
        except OSError:
            servidor.close()
            self.arquivo_log.close()
            raise

        print(f"READY {self.host} {self.porta}", flush=True)
        interface_thread = threading.Thread(target=self.interface, name="interface", daemon=True)
        interface_thread.start()

        threads_clientes = []
        try:
            while not self.encerrar.is_set():
                try:
                    conexao, endereco = servidor.accept()
                except socket.timeout:
                    continue
                except OSError:
                    if self.encerrar.is_set():
                        break
                    raise

                conexao.settimeout(0.5)
                thread = threading.Thread(
                    target=self.tratar_cliente,
                    args=(conexao, endereco),
                    name=f"cliente-{endereco[1]}",
                    daemon=True,
                )
                threads_clientes.append(thread)
                thread.start()
        finally:
            self.encerrar.set()
            servidor.close()

            with self.lock:
                conexoes = list(self.sockets.values())
            for conexao in conexoes:
                try:
                    conexao.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    conexao.close()
                except OSError:
                    pass

            for thread in threads_clientes:
                thread.join(timeout=1.0)
            self.arquivo_log.close()


def main():
    parser = argparse.ArgumentParser(description="Coordenador FIFO de exclusão mútua distribuída")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=50000)
    parser.add_argument("--log", default="coordenador.log")
    args = parser.parse_args()

    coordenador = Coordenador(args.host, args.port, args.log)
    try:
        coordenador.executar()
    except OSError as erro:
        print(f"ERRO Não foi possível iniciar o coordenador: {erro}", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
