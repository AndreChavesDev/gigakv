# ⚡ GigaKV - Distributed Raft-Based Key-Value Store in Go

![Go Version](https://img.shields.io/badge/Go-1.22%2B-00ADD8?style=flat&logo=go)
![Architecture](https://img.shields.io/badge/Architecture-Distributed%20Consensus-blue)
![Protocol](https://img.shields.io/badge/Protocol-gRPC%20%2F%20Protobuf-00599C)
![License](https://img.shields.io/badge/License-MIT-green)

O **GigaKV** é um sistema de armazenamento Chave-Valor distribuído, tolerante a falhas e altamente disponível, desenvolvido em **Go**. O projeto implementa o algoritmo de consenso **Raft** para garantir a consistência forte (*Linearizability*) dos dados replicados em múltiplos nós, utilizando **gRPC** com **Protocol Buffers** para a comunicação entre nós e clientes, e **Write-Ahead Logging (WAL)** para persistência durável no disco local.

---

## 📑 Sumário

- [Visão Geral & Casos de Uso](#visão-geral--casos-de-uso)
- [Algoritmos e Fundamentos Teóricos](#algoritmos-e-fundamentos-teóricos)
- [Arquitetura e Estrutura de Pastas](#arquitetura-e-estrutura-de-pastas)
- [Desafios Técnicos e Soluções](#desafios-técnicos-e-soluções)
- [Validação Prática e Relatório de Testes](#validação-prática-e-relatório-de-testes)
- [Guia de Execução Local](#guia-de-execução-local)
- [Guia de Publicação e Versionamento (Git & GitHub)](#guia-de-publicação-e-versionamento-git--github)
- [Trabalhos Futuros (Future Work)](#trabalhos-futuros-future-work)
- [Licença e Autoria](#licença-e-autoria)

---

## Visão Geral & Casos de Uso

### O Problema da Consistência Distribuída
Em arquiteturas de microsserviços e sistemas distribuídos, manter dados idênticos em múltiplos servidores é um desafio complexo. Se um servidor falhar, perder conexão de rede ou reiniciar, o sistema precisa continuar operando sem perder informações ou fornecer dados desatualizados (*stale reads*).

O **GigaKV** resolve esse problema fornecendo uma máquina de estados replicada. Toda alteração enviada ao cluster é distribuída através do consenso Raft, garantindo que a maioria dos nós concorde com a ordem exata das operações antes que elas sejam confirmadas ao cliente.

### Casos de Uso Práticos

* **Gerenciamento Centralizado de Configurações:** Armazenamento de *feature flags* e parâmetros operacionais de microsserviços em tempo real (semelhante ao papel do *etcd* no Kubernetes ou do *HashiCorp Consul*).
* **Coordenação de Serviços & Service Discovery:** Registro de instâncias ativas e checagem de saúde em clusters distribuídos.
* **Locks Distribuídos e Eleição de Líder Interna:** Sincronização de tarefas concorrentes em sistemas distribuídos para evitar *race conditions* entre microsserviços.

---

## Algoritmos e Fundamentos Teóricos

### 1. O Algoritmo de Consenso Raft
O Raft divide o problema de consenso em três subproblemas gerenciáveis:

1. **Eleição de Líder (*Leader Election*):** 
   - Quando o cluster inicia, todos os nós começam no papel de **Follower**.
   - Se um Follower não receber sinais periódicos (*heartbeats*) do Líder durante o *Election Timeout* (aleatorizado entre 150ms e 300ms), ele se promove a **Candidate**, incrementa o **Termo** (relógio lógico) e solicita votos aos pares.
   - O nó que obter a maioria dos votos torna-se o novo **Leader**.

2. **Replicação de Logs (*Log Replication*):**
   - O Líder é o único nó que aceita requisições de escrita (`Put`) dos clientes.
   - Ao receber uma chave e valor, o Líder adiciona a operação ao seu log local não consolidado (*uncommitted*) e envia RPCs `AppendEntries` em paralelo para todos os seguidores.
   - Assim que a maioria dos nós confirma o recebimento da entrada no log, o Líder consolida a operação (*commit*) e notifica os seguidores para aplicarem a alteração em suas máquinas de estado locais.

3. **Garantia de Segurança (*Safety Invariants*):**
   - **Leader Append-Only:** O Líder nunca sobrescreve ou trunca suas próprias entradas de log.
   - **Log Matching Property:** Se dois logs contêm uma entrada com o mesmo índice e termo, eles são idênticos em todos os índices anteriores.
   - **Leader Completeness:** Se uma entrada de log foi consolidada em um determinado Termo, ela estará presente nos logs dos Líderes de todos os Termos superiores.

### 2. Modelo Matemático de Quórum e Tolerância a Falhas
Para evitar o problema de *Split-Brain* (onde duas partições de rede elegem dois líderes concorrentes), o Raft exige que qualquer decisão seja tomada pela maioria absoluta dos nós.

O quórum mínimo $Q$ necessário para validar uma transação é dado por:

$$Q = \left\lfloor \frac{N}{2} \right\rfloor + 1$$

Onde $N$ é o número total de nós no cluster. A quantidade máxima de falhas simultâneas $F$ que o cluster pode tolerar sem perder a disponibilidade é:

$$N = 2F + 1 \implies F = \left\lfloor \frac{N - 1}{2} \right\rfloor$$

#### Aplicação Prática no GigaKV:
* Para um cluster com **$N = 3$ nós**:
  * Quórum mínimo $Q = \lfloor 3/2 \rfloor + 1 = 2$ nós.
  * Tolerância a falhas $F = 1$ nó.
  * Se 1 nó cair, os 2 nós remanescentes continuam operando normalmente.

---

## Arquitetura e Estrutura de Pastas

A arquitetura do GigaKV é dividida em camadas bem definidas: **Interface Externa (gRPC/Protobuf)**, **Núcleo de Consenso (Raft Engine)** e **Camada de Persistência (Storage/WAL)**.

```text
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
│   │   ├── node.go      # Máquina de estados, lógica do timer, eleição e commitLogs
│   │   └── state.go     # Gerenciamento de papéis (Leader, Follower, Candidate)
│   ├── storage/         # Mecanismo de persistência durável (WAL e Engine KV em memória)
│   │   └── engine.go
│   └── server/          # Implementação dos serviços gRPC expostos para clientes
│       └── kv.go
├── docker-compose.yml   # Arquivo de orquestração para execução em containers
├── .gitignore           # Regras de exclusão do Git para binários e arquivos de log
├── go.mod               # Gerenciamento de módulos e dependências do Go
├── go.sum
└── README.md            # Documentação técnica e relatório do projeto
