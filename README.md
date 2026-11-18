# Exclusão mútua distribuída centralizada

Implementação simples em Python 3, com sockets TCP, processos clientes e uma fila FIFO no coordenador.

## Arquivos

- `coordenador.py`: aceita conexões, processa mensagens, mantém a fila e oferece a interface de terminal.
- `processo.py`: solicita acesso, escreve no arquivo compartilhado e libera a região crítica.
- `protocolo.py`: mensagens de tamanho fixo e funções para enviar/receber quadros TCP.
- `executar.py`: inicia os processos e verifica o resultado e o log.

## Executar um cenário

No terminal, entre nesta pasta e execute:

```powershell
python executar.py --n 3 --r 2 --k 1
```

Parâmetros:

- `--n`: número de processos (padrão: 3).
- `--r`: repetições por processo (padrão: 2).
- `--k`: segundos de espera dentro da região crítica (padrão: 1).
- `--host`: endereço do coordenador (padrão: `127.0.0.1`).
- `--port`: porta TCP (padrão: `50000`).
- `--output-dir`: pasta para `resultado.txt` e `coordenador.log`.

O script inicia o coordenador, espera o socket ficar pronto, inicia os clientes sequencialmente sem atraso intencional, aguarda o término e executa as verificações. Uma execução comum grava `resultado.txt` e `coordenador.log` nesta pasta, substituindo os arquivos anteriores.

## Executar os casos de demonstração

```powershell
python executar.py --suite
```

A suíte executa os quatro cenários do planejamento e guarda os arquivos de cada caso em uma subpasta de `resultados/`.

## Interface do coordenador

O coordenador oferece estes comandos no terminal:

1. Imprimir a fila atual. O primeiro item está na região crítica e os seguintes aguardam.
2. Imprimir quantas vezes cada processo recebeu uma concessão.
3. Encerrar o coordenador.

O script de execução envia o comando 3 automaticamente depois que os processos clientes terminam. Para executar coordenador e processos manualmente em terminais separados, use:

```powershell
python coordenador.py --host 127.0.0.1 --port 50000 --log coordenador.log
```

Em outros terminais, inicie um processo por vez:

```powershell
python processo.py --id 1 --repetitions 2 --k 1 --host 127.0.0.1 --port 50000 --result resultado.txt
python processo.py --id 2 --repetitions 2 --k 1 --host 127.0.0.1 --port 50000 --result resultado.txt
```

As mensagens usam F = 32 bytes ASCII, com identificador numérico do tipo (1 = REQUEST, 2 = GRANT, 3 = RELEASE), identificador do processo e preenchimento com zeros, seguindo o formato de exemplo do enunciado. O campo de repetição fica no log do coordenador, não na mensagem, como no pseudocódigo da aula.

## Limites e observações

- A fila `Q` contém o dono atual na cabeça e os pedidos seguintes em ordem FIFO, como no pseudocódigo da aula. Não há uma variável separada para o dono.
- O coordenador mantém os sockets necessários para responder aos processos e os contadores exigidos pela interface.
- O coordenador central é um ponto único de falha. A implementação não tenta recuperar pedidos se um processo desconectar durante a região crítica.
- A avaliação dos horários usa ordem não decrescente porque a precisão gravada é de milissegundos.
