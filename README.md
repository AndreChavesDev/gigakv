# ⚡ GigaKV — Distributed Raft-Based Key-Value Store in Go

![Go Version](https://img.shields.io/badge/Go-1.21%2B-00ADD8?logo=go)
![Architecture](https://img.shields.io/badge/architecture-Raft%20Consensus-blueviolet)
![Protocol](https://img.shields.io/badge/protocol-gRPC%20%2F%20Protobuf-orange)
![License](https://img.shields.io/badge/license-MIT-green)

O **GigaKV** é um sistema de armazenamento Chave-Valor (Key-Value) distribuído, tolerante a falhas e altamente disponível, desenvolvido inteiramente em **Go**. O projeto implementa o algoritmo de consenso **Raft** para garantir consistência forte (*Linearizability*) dos dados replicados entre múltiplos nós, utilizando **gRPC** com **Protocol Buffers** para comunicação entre nós e clientes, e **Write-Ahead Logging (WAL)** para persistência durável em disco local.

---

## 📑 Sumário

- [Visão Geral & Casos de Uso](#-visão-geral--casos-de-uso)
- [Algoritmos e Fundamentos Teóricos](#-algoritmos-e-fundamentos-teóricos)
- [Arquitetura e Estrutura de Pastas](#-arquitetura-e-estrutura-de-pastas)
- [Desafios Técnicos e Soluções](#-desafios-técnicos-e-soluções-de-implementação)
- [Validação Prática e Relatório de Testes](#-validação-prática-e-relatório-de-testes)
- [Guia de Execução Local](#-guia-de-execução-local)
- [Trabalhos Futuros](#-trabalhos-futuros-future-work)
- [Licença e Autoria](#-licença-e-autoria)

---

## 🧭 Visão Geral & Casos de Uso

### O Problema da Consistência Distribuída

Em arquiteturas de microsserviços e sistemas distribuídos, manter dados idênticos em múltiplos servidores é um desafio complexo. Se um servidor falhar, perder conexão de rede ou reiniciar, o sistema precisa continuar operando sem perder informações ou fornecer dados desatualizados (*stale reads*).

O GigaKV resolve esse problema fornecendo uma **máquina de estados replicada**. Toda alteração enviada ao cluster é distribuída através do consenso Raft, garantindo que a maioria dos nós concorde com a ordem exata das operações antes que elas sejam confirmadas ao cliente.

### Casos de Uso Práticos

- **Gerenciamento Centralizado de Configurações** — armazenamento de *feature flags* e parâmetros operacionais de microsserviços em tempo real (papel semelhante ao do `etcd` no Kubernetes ou do HashiCorp Consul).
- **Coordenação de Serviços & Service Discovery** — registro de instâncias ativas e checagem de saúde em clusters distribuídos.
- **Locks Distribuídos e Eleição de Líder Interna** — sincronização de tarefas concorrentes para evitar *race conditions* entre microsserviços.

---

## 📚 Algoritmos e Fundamentos Teóricos

### 1. O Algoritmo de Consenso Raft

O Raft decompõe o problema de consenso distribuído em subproblemas gerenciáveis:

#### Eleição de Líder (*Leader Election*)

O cluster opera com nós em três estados possíveis: **Follower**, **Candidate** e **Leader**.

- Todos os nós iniciam como *Followers*.
- Caso um nó deixe de receber sinais de vida (*heartbeats*) dentro de um intervalo de tempo aleatorizado (**Election Timeout**, configurado entre 150ms e 300ms), ele transita para o estado *Candidate*.
- O candidato incrementa o termo lógico (**Term**), vota em si mesmo e despacha requisições de voto para os demais nós.
- O primeiro nó a acumular a maioria absoluta dos votos assume o papel de **Leader**, iniciando a emissão contínua de *heartbeats*.

#### Replicação de Logs (*Log Replication*)

O *Leader* centraliza o recebimento de mutações de escrita (`Put`):

1. O *Leader* anexa a operação ao seu log local como entrada não consolidada (*uncommitted*).
2. Dispara mensagens RPC do tipo `AppendEntries` em paralelo para todos os nós *Followers*.
3. Assim que a entrada é confirmada pela maioria dos nós do cluster (atingindo o quórum), o *Leader* consolida (*commit*) a transação, aplica-a à sua máquina de estados interna e avisa os seguidores para fazerem o mesmo.

#### Garantias de Segurança (*Safety Invariants*)

- **Leader Append-Only**: o Líder nunca sobrescreve ou trunca suas próprias entradas de log.
- **Log Matching Property**: se dois logs contêm uma entrada com o mesmo índice e termo, eles são idênticos em todos os índices anteriores.
- **Leader Completeness**: se uma entrada de log foi consolidada em um determinado Termo, ela estará presente nos logs dos Líderes de todos os Termos superiores.

### 2. Modelo Matemático de Quórum e Tolerância a Falhas

Para prevenir o fenômeno de **Split-Brain** — onde duas metades da rede operam de forma isolada elegendo líderes distintos — o sistema exige quórum estrito para qualquer alteração de estado:

$$Q = \left\lfloor \frac{N}{2} \right\rfloor + 1$$

Onde $N$ representa o número total de instâncias no cluster. A capacidade máxima de tolerância a falhas simultâneas ($F$) é calculada por:

$$N = 2F + 1 \implies F = \left\lfloor \frac{N - 1}{2} \right\rfloor$$

**Aplicação prática no GigaKV:** em uma topologia padrão de $N = 3$ nós (adotada nos testes do projeto):

| Métrica | Valor |
|---|---|
| Quórum mínimo ($Q$) | 2 nós |
| Tolerância a falhas ($F$) | 1 nó |

Ou seja: se 1 nó cair, os 2 nós remanescentes continuam operando normalmente, sem perda de disponibilidade nem de consistência.

---

## 🏗️ Arquitetura e Estrutura de Pastas

A arquitetura do GigaKV é dividida em três camadas bem delimitadas: **Interface Externa** (gRPC/Protobuf), **Núcleo de Consenso** (Raft Engine) e **Camada de Persistência** (Storage/WAL).

```
gigakv/
├── api/                 # Definições do Protocol Buffers e código Go gerado (gRPC)
│   └── gigakv.pb.go
├── cmd/
│   ├── client/          # Cliente CLI em Go para interagir com o cluster via gRPC
│   │   └── main.go
│   └── server/          # Ponto de entrada para inicialização dos servidores Raft
│       └── main.go
├── internal/
│   ├── raft/            # Núcleo do algoritmo Raft
│   │   ├── node.go      # Máquina de estados, lógica de timer, eleição e commitLogs
│   │   └── state.go     # Gerenciamento de papéis (Leader, Follower, Candidate)
│   ├── storage/         # Mecanismo de persistência durável (WAL e engine KV em memória)
│   │   └── engine.go
│   └── server/          # Implementação dos serviços gRPC expostos aos clientes
│       └── kv.go
├── docker-compose.yml   # Orquestração para execução em containers
├── .gitignore           # Regras de exclusão do Git para binários e arquivos de log
├── go.mod               # Gerenciamento de módulos e dependências do Go
├── go.sum
└── README.md            # Este documento
```

- **Camada API (`/api`)** — contratos de interface definidos via Protocol Buffers, estabelecendo os tipos de mensagens transmitidas nas RPCs de consenso e nos comandos de clientes (`Put` / `Get`).
- **Camada de Consenso (`/internal/raft`)** — núcleo lógico do Raft: controla persistência de metadados de termo, contadores de votos, temporizadores de *timeout* e o fluxo de varredura/aplicação de logs.
- **Camada de Armazenamento (`/internal/storage`)** — gerencia a estrutura de dados em memória e a persistência sequencial em disco através do WAL.

---

## 🛠️ Desafios Técnicos e Soluções de Implementação

O desenvolvimento do motor de consenso em Go exigiu o tratamento rigoroso de desafios inerentes à concorrência de baixo nível e a sistemas distribuídos.

### Mitigação de Condições de Corrida (*Race Conditions*)

**Desafio:** o modelo concorrente do Go, aliado à natureza assíncrona do Raft (múltiplas *goroutines* executando timers, escutas gRPC de clientes e despacho de *heartbeats* simultaneamente), gerava inconsistências iniciais onde um comando de escrita já confirmado retornava falha de leitura imediata (*stale read*).

**Solução:** implementação de mecanismos estritos de exclusão mútua (`sync.Mutex` e `sync.RWMutex`) e redesenho da rotina de consolidação de logs (`commitLogs`), assegurando que a resposta ao cliente seja emitida apenas após a gravação síncrona e atômica na máquina de estados e no disco.

### Sincronização Temporal e Latência de Rede

**Desafio:** a latência inerente à infraestrutura de rede local em ambiente Windows gerava disparo prematuro de *timeouts*, causando eleições espúrias e ciclos de votação dividida (*split votes*).

**Solução:** ajuste fino do intervalo de *heartbeat* para 50ms e aleatorização do *Election Timeout* na faixa de 150ms a 300ms, garantindo estabilidade e convergência rápida do líder.

### Garantia de I/O Atômico no WAL

**Desafio:** operações de escrita em disco vulneráveis a interrupções abruptas de energia ou encerramento de processos poderiam corromper o log de transações.

**Solução:** forçagem de sincronização de hardware utilizando a instrução `file.Sync()` imediatamente após cada *append* no arquivo de log do Write-Ahead Log.

---

## ✅ Validação Prática e Relatório de Testes

O cluster foi submetido a cenários rigorosos de estresse utilizando uma topologia local de 3 nós nas portas `:8001`, `:8002` e `:8003`.

| Cenário de Teste | Condição Aplicada | Comportamento Observado / Resultado |
|---|---|---|
| **Failover de Líder** | Encerramento abrupto (`kill -9`) do nó atuando como Leader. | Os nós remanescentes detectaram a ausência de *heartbeats*, esgotaram o *timeout*, elegeram um novo líder em menos de 300ms e restabeleceram o recebimento de escritas sem perda de dados. |
| **Replicação e Quórum** | Envio de mutações (`Put`) direcionadas ao líder e consultas de leitura (`Get`) direcionadas propositalmente aos seguidores. | A consistência linearizável foi mantida. Nenhuma leitura retornou dados obsoletos, pois o líder aguardou o reconhecimento de ao menos um seguidor ($Q=2$) antes de consolidar o log. |
| **Recuperação de Crash** | Encerramento simultâneo de todo o cluster e reinicialização posterior. | Através da leitura dos arquivos `.metadata` e `.wal` armazenados em disco, os nós recuperaram o último termo válido, os votos computados e reconstruíram integralmente a árvore de dados em memória. |

---

## 🚀 Guia de Execução Local

### Pré-requisitos

- [Go](https://go.dev/dl/) 1.21 ou superior
- [Docker](https://www.docker.com/) e Docker Compose (opcional, para execução em containers)
- `protoc` (Protocol Buffers Compiler), caso deseje regenerar os stubs gRPC

### Clonando o repositório

```bash
git clone https://github.com/<seu-usuario>/gigakv.git
cd gigakv
```

### Instalando as dependências

```bash
go mod download
```

### Executando um cluster local de 3 nós

Abra três terminais distintos e inicie cada nó apontando para os endereços dos demais membros do cluster:

```bash
# Terminal 1
go run ./cmd/server --id=node1 --port=8001 --peers=localhost:8002,localhost:8003

# Terminal 2
go run ./cmd/server --id=node2 --port=8002 --peers=localhost:8001,localhost:8003

# Terminal 3
go run ./cmd/server --id=node3 --port=8003 --peers=localhost:8001,localhost:8002
```

> Após alguns instantes, um dos nós será eleito **Leader**. Os logs no terminal indicarão a transição de estado (`Follower → Candidate → Leader`).

### Executando via Docker Compose

```bash
docker-compose up --build
```

### Utilizando o cliente CLI

```bash
# Escrever um par chave-valor (a requisição é redirecionada automaticamente ao líder)
go run ./cmd/client put minha-chave "meu-valor"

# Ler um valor
go run ./cmd/client get minha-chave
```

---


**Boas práticas recomendadas:**

- Utilize um `.gitignore` adequado para Go, excluindo binários compilados, arquivos `.wal`/`.metadata` gerados em testes locais e diretórios de build.
- Adote *commits* semânticos (`feat:`, `fix:`, `docs:`, `test:`, `refactor:`) para manter o histórico organizado.
- Utilize *branches* de funcionalidade (`feature/nome-da-feature`) e *pull requests* para revisão de código antes de integrar ao `main`.
- Marque versões estáveis com *tags* semânticas (ex.: `v0.1.0`) usando `git tag` e `git push --tags`.

---

## 🔭 Trabalhos Futuros (Future Work)

O GigaKV cumpriu com êxito o objetivo de consolidar os fundamentos teóricos de sistemas distribuídos e algoritmos de consenso tolerantes a falhas em uma implementação prática de alta performance em Go. Para a evolução do projeto rumo a um ambiente de produção de larga escala, as seguintes melhorias arquiteturais estão planejadas:

- **Compactação de Log e Snapshots (`InstallSnapshot`)**, implementação de pontos de salvamento de estado (*snapshots*) para podar o histórico do WAL, evitando o crescimento indefinido do armazenamento em disco.
- **Otimização de Leituras (Lease Read / ReadIndex)**, eliminação do *overhead* de fluxo de log em operações estritamente de leitura (`Get`), permitindo que o líder responda a consultas locais com latência adicional zero, sem violar a linearizabilidade.
- **Proxy Transparente em Seguidores**, permitir que nós *Followers* recebam requisições de clientes e façam o encaminhamento (*proxying*) automático para o líder ativo no cluster.
- **Integração com Motor LSM-Tree**, substituição do mapa em memória RAM por um mecanismo de armazenamento otimizado para disco baseado em *Log-Structured Merge-Tree* (como BadgerDB ou Pebble).

---

## 📄 Licença e Autoria

Este projeto está licenciado sob os termos da licença **MIT** — sinta-se livre para usar, modificar e distribuir, desde que os créditos originais sejam mantidos. Veja o arquivo `LICENSE` para mais detalhes.

Desenvolvido como projeto de estudo aprofundado em sistemas distribuídos, algoritmos de consenso e programação concorrente em Go.

Contribuições são bem-vindas! Sinta-se à vontade para abrir *issues* ou enviar *pull requests*.
