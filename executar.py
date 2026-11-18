"""Executa processos concorrentes e confere os requisitos de avaliação."""

import argparse
import csv
from collections import Counter
from datetime import datetime
from pathlib import Path
import subprocess
import sys


PASTA = Path(__file__).resolve().parent
COORDENADOR = PASTA / "coordenador.py"
PROCESSO = PASTA / "processo.py"
CASOS = [
    (1, 2, 0.0),
    (3, 1, 1.0),
    (3, 2, 1.0),
    (5, 2, 0.0),
]


def validar_resultado(caminho, n, r):
    linhas = caminho.read_text(encoding="utf-8").splitlines()
    ids = []
    horarios = []
    formato_correto = True

    for linha in linhas:
        try:
            id_texto, horario_texto = linha.split(";", 1)
            ids.append(int(id_texto))
            horarios.append(datetime.fromisoformat(horario_texto))
        except (ValueError, TypeError):
            formato_correto = False
            break

    contagens = Counter(ids)
    quantidade_ok = len(linhas) == n * r
    contagens_ok = all(contagens[pid] == r for pid in range(1, n + 1)) and len(contagens) == n
    ordem_ok = formato_correto and all(a <= b for a, b in zip(horarios, horarios[1:]))

    return [
        ("resultado: n × r linhas", quantidade_ok, f"{len(linhas)} linha(s); esperado {n * r}"),
        ("resultado: r linhas por processo", contagens_ok, str(dict(sorted(contagens.items())))),
        ("resultado: id e timestamp válidos", formato_correto and len(horarios) == len(linhas), "formato id;timestamp"),
        ("resultado: timestamps não decrescentes", ordem_ok, "ordem do relógio do sistema"),
    ]


def validar_log(caminho, n, r):
    with caminho.open("r", newline="", encoding="utf-8") as arquivo:
        eventos = list(csv.DictReader(arquivo, delimiter=";"))

    pedidos = [e for e in eventos if e["direcao"] == "RX" and e["tipo"] == "REQUEST"]
    grants = [e for e in eventos if e["direcao"] == "TX" and e["tipo"] == "GRANT"]
    releases = [e for e in eventos if e["direcao"] == "RX" and e["tipo"] == "RELEASE"]
    esperado = n * r

    def pares(registros):
        return [(int(e["processo"]), int(e["repeticao"])) for e in registros]

    pedido_pares = pares(pedidos)
    grant_pares = pares(grants)
    release_pares = pares(releases)
    tipos_acesso = [
        e["tipo"] for e in eventos
        if (e["direcao"] == "TX" and e["tipo"] == "GRANT")
        or (e["direcao"] == "RX" and e["tipo"] == "RELEASE")
    ]
    alternancia_ok = tipos_acesso == [tipo for _ in range(esperado) for tipo in ("GRANT", "RELEASE")]
    linhas_mensagem_ok = all(e["origem"] and e["destino"] for e in pedidos + grants + releases)

    return [
        ("log: REQUEST, GRANT e RELEASE", len(pedidos) == esperado and len(grants) == esperado and len(releases) == esperado,
         f"{len(pedidos)} REQUEST, {len(grants)} GRANT, {len(releases)} RELEASE; esperado {esperado} de cada"),
        ("log: GRANT/RELEASE intercalados", alternancia_ok, "cada concessão tem liberação antes da próxima"),
        ("log: ordem REQUEST = RELEASE", pedido_pares == release_pares, "pares (processo, repetição)"),
        ("log: GRANT corresponde a RELEASE", grant_pares == release_pares, "pares (processo, repetição)"),
        ("log: origem e destino registrados", linhas_mensagem_ok, "campos presentes em todas as mensagens"),
    ]


def encerrar_coordenador(coordenador):
    if coordenador.poll() is not None:
        return
    try:
        coordenador.stdin.write("3\n")
        coordenador.stdin.flush()
        coordenador.communicate(timeout=5)
    except (BrokenPipeError, subprocess.TimeoutExpired):
        coordenador.terminate()
        try:
            coordenador.communicate(timeout=3)
        except subprocess.TimeoutExpired:
            coordenador.kill()
            coordenador.communicate()


def executar_caso(n, r, k, host, porta, pasta_saida):
    pasta_saida.mkdir(parents=True, exist_ok=True)
    caminho_resultado = pasta_saida / "resultado.txt"
    caminho_log = pasta_saida / "coordenador.log"
    caminho_resultado.write_text("", encoding="utf-8")

    print(f"\nCaso: n={n}, r={r}, k={k:g}s; porta={porta}", flush=True)
    coordenador = subprocess.Popen(
        [
            sys.executable, "-u", str(COORDENADOR), "--host", host,
            "--port", str(porta), "--log", str(caminho_log),
        ],
        cwd=PASTA,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        bufsize=1,
    )

    linha_ready = coordenador.stdout.readline().strip()
    if not linha_ready.startswith("READY "):
        saida, _ = coordenador.communicate(timeout=3)
        raise RuntimeError(f"Coordenador não iniciou: {linha_ready}\n{saida}")
    print(f"Coordenador pronto em {host}:{porta}.", flush=True)

    clientes = []
    try:
        # Popen é chamado sequencialmente sem sleep entre os inícios.
        for pid in range(1, n + 1):
            clientes.append(subprocess.Popen([
                sys.executable, str(PROCESSO),
                "--id", str(pid),
                "--repetitions", str(r),
                "--k", str(k),
                "--host", host,
                "--port", str(porta),
                "--result", str(caminho_resultado),
            ], cwd=PASTA))

        timeout = max(30.0, n * r * (k + 2.0) + 15.0)
        codigos = []
        for cliente in clientes:
            try:
                codigos.append(cliente.wait(timeout=timeout))
            except subprocess.TimeoutExpired:
                for ativo in clientes:
                    if ativo.poll() is None:
                        ativo.kill()
                raise RuntimeError("Tempo limite excedido aguardando os processos clientes.")
    finally:
        encerrar_coordenador(coordenador)

    processos_ok = all(codigo == 0 for codigo in codigos)
    verificacoes = validar_resultado(caminho_resultado, n, r)
    verificacoes.extend(validar_log(caminho_log, n, r))
    verificacoes.append(("processos clientes concluíram", processos_ok, f"códigos de saída: {codigos}"))

    for nome, passou, detalhe in verificacoes:
        status = "OK" if passou else "FALHOU"
        print(f"[{status}] {nome}: {detalhe}")

    return all(passou for _, passou, _ in verificacoes)


def main():
    parser = argparse.ArgumentParser(description="Executa e avalia o trabalho de exclusão mútua distribuída")
    parser.add_argument("--n", type=int, default=3, help="número de processos")
    parser.add_argument("--r", type=int, default=2, help="repetições por processo")
    parser.add_argument("--k", type=float, default=1.0, help="espera em segundos dentro da região crítica")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=50000)
    parser.add_argument("--output-dir", type=Path, default=None,
                        help="pasta para resultado.txt e coordenador.log")
    parser.add_argument("--suite", action="store_true", help="executa os quatro cenários de demonstração")
    args = parser.parse_args()

    if args.n < 1 or args.r < 1 or args.k < 0:
        parser.error("n e r devem ser positivos; k não pode ser negativo")
    if args.port < 1 or args.port > 65535:
        parser.error("--port deve estar entre 1 e 65535")
    if args.output_dir is None:
        args.output_dir = PASTA / "resultados" if args.suite else PASTA
    args.output_dir = args.output_dir.resolve()

    if args.suite:
        casos = CASOS
    else:
        casos = [(args.n, args.r, args.k)]

    resultados = []
    for indice, (n, r, k) in enumerate(casos):
        if args.suite:
            pasta_saida = args.output_dir / f"caso_{indice + 1}_n{n}_r{r}_k{k:g}"
            porta = args.port + indice
        else:
            pasta_saida = args.output_dir
            porta = args.port
        if porta > 65535:
            parser.error("A suíte ultrapassa o limite de portas TCP.")
        try:
            passou = executar_caso(n, r, k, args.host, porta, pasta_saida)
        except (OSError, RuntimeError) as erro:
            print(f"FALHA na execução: {erro}", file=sys.stderr)
            return 1
        resultados.append(passou)

    print(f"\nResumo: {sum(resultados)}/{len(resultados)} caso(s) passaram.")
    return 0 if all(resultados) else 1


if __name__ == "__main__":
    sys.exit(main())
