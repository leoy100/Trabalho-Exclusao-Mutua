"""Formato simples de mensagens usado por coordenador.py e processo.py."""

F = 32

REQUEST = 1
GRANT = 2
RELEASE = 3

NOMES = {
    REQUEST: "REQUEST",
    GRANT: "GRANT",
    RELEASE: "RELEASE",
}


class ErroProtocolo(ValueError):
    pass


def criar_mensagem(tipo, identificador_processo):
    """Cria um quadro ASCII de exatamente F bytes, com preenchimento em zeros."""
    if tipo not in NOMES:
        raise ErroProtocolo(f"Tipo de mensagem inválido: {tipo}")
    if not isinstance(identificador_processo, int) or identificador_processo < 1:
        raise ErroProtocolo("O identificador do processo deve ser um inteiro positivo.")

    conteudo = f"{tipo}|{identificador_processo}|".encode("ascii")
    if len(conteudo) > F:
        raise ErroProtocolo(f"A mensagem excede o limite de {F} bytes.")
    return conteudo.ljust(F, b"0")


def receber_mensagem(sock):
    """Lê exatamente F bytes de um socket TCP e retorna (tipo, id), ou None no EOF."""
    quadro = bytearray()
    while len(quadro) < F:
        parte = sock.recv(F - len(quadro))
        if not parte:
            if not quadro:
                return None
            raise ErroProtocolo("Conexão encerrada no meio de uma mensagem.")
        quadro.extend(parte)

    try:
        texto = quadro.decode("ascii")
    except UnicodeDecodeError as erro:
        raise ErroProtocolo("A mensagem contém bytes que não são ASCII.") from erro

    partes = texto.split("|", 2)
    if len(partes) != 3 or not partes[2] or any(c != "0" for c in partes[2]):
        raise ErroProtocolo("Formato ou preenchimento da mensagem inválido.")

    try:
        tipo = int(partes[0])
        identificador = int(partes[1])
    except ValueError as erro:
        raise ErroProtocolo("Tipo e identificador devem ser numéricos.") from erro

    if tipo not in NOMES:
        raise ErroProtocolo(f"Tipo de mensagem desconhecido: {tipo}")
    if identificador < 1:
        raise ErroProtocolo("O identificador do processo deve ser positivo.")
    return tipo, identificador


def enviar_mensagem(sock, tipo, identificador_processo):
    sock.sendall(criar_mensagem(tipo, identificador_processo))
