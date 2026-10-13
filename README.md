# T1 — Problemas Clássicos de Concorrência (FPPD 98713-04)

Arthur Domingues Fachel Nunes

Dois programas Go independentes: o jantar dos filósofos e o
produtor/consumidor com buffer limitado. Só biblioteca padrão, nenhuma
dependência externa.

```
t1/
├── go.mod
├── filosofos/              Problema 1
│   ├── mesa.go             garfos como channels, detector de deadlock, as 4 estratégias
│   ├── main.go             CLI, métricas, CSV, conferência de encerramento
│   └── mesa_test.go
├── produtor-consumidor/    Problema 2
│   ├── buffer.go           versão channel, semáforo FIFO, versão com semáforos
│   ├── main.go             CLI, consumidor com timeout, verificação item a item, CSV
│   └── buffer_test.go
├── scripts/
│   ├── coletar.sh          regera todos os CSVs de resultados/
│   └── resumo.py           tabelas (média ± desvio) a partir dos CSVs, só stdlib
├── resultados/             CSVs brutos + resumo.txt
└── relatorio/              relatorio.md
```

## O que você precisa ter instalado

Go 1.22 ou mais novo, porque o código usa `math/rand/v2` e a semântica nova
de variável de laço. O `-race` precisa de cgo, ou seja, de um compilador C
(`gcc`) no PATH. Testei com Go 1.23.4 em linux/amd64.

## Compilar e executar

```sh
cd t1
go build ./...

# Problema 1: N filósofos, R refeições cada, as 4 versões em sequência
go run -race ./filosofos -n 5 -r 100

# uma estratégia só
go run -race ./filosofos -n 5 -r 100 -estrategia hierarquia

# forçar o deadlock da base (sem pensar, com pausa maior entre os garfos)
go run -race ./filosofos -n 5 -r 100 -estrategia base -pensar 0 -pausa 2ms

# Problema 2: P produtores, C consumidores, K = 1, 10 e 100, as 2 versões
go run -race ./produtor-consumidor -p 4 -c 4 -m 500 -k 1,10,100

# ver o consumidor 0 caindo no timeout (produção lenta de propósito)
go run -race ./produtor-consumidor -p 1 -c 2 -m 30 -k 5 -produzir 5ms -timeout 2ms -v

# testes, incluindo os casos adversariais, com detector de race
go test -race ./...
```

## Flags

`filosofos`

| flag | padrão | o que faz |
|---|---|---|
| `-n` | 5 | número de filósofos |
| `-r` | 100 | refeições por filósofo |
| `-pool` | false | troca o R por filósofo por um orçamento global de N*R refeições disputado por todos. É esse modo que faz a contagem de refeições virar medida de fairness |
| `-estrategia` | todas | `base`, `hierarquia`, `limite`, `garcom`, ou uma lista separada por vírgula |
| `-pensar` | 2ms | tempo de pensar, uniforme em [0, pensar] |
| `-comer` | 1ms | tempo de comer |
| `-pausa` | 1ms | pausa entre pegar o 1º e o 2º garfo |
| `-garcom-reserva` | true | `false` desliga a reserva FIFO do garçom. A variante gulosa existe só para mostrar a starvation que a reserva evita |
| `-reps`, `-seed`, `-csv`, `-q` | | repetições, semente, saída CSV, modo silencioso |

`produtor-consumidor`

| flag | padrão | o que faz |
|---|---|---|
| `-p`, `-c` | 4, 4 | produtores e consumidores. O consumidor 0 é o que usa `select` + `time.After` |
| `-k` | 1,10,100 | capacidades do buffer a testar |
| `-m` | 500 | itens por produtor |
| `-produzir`, `-consumir` | 1ms, 1ms | média (exponencial) do trabalho por item. 0 desliga |
| `-timeout` | 3ms | prazo do consumidor 0 |
| `-versao` | todas | `channel`, `semaforo` |
| `-reps`, `-seed`, `-csv`, `-v` | | |

## Código de saída

Os dois programas só terminam com 0 se passarem nas conferências de
encerramento ordenado:

- todas as goroutines criadas terminaram, comparando com o
  `runtime.NumGoroutine()` do início;
- todos os garfos voltaram para a mesa (Problema 1);
- cada item produzido foi consumido exatamente uma vez (Problema 2).

Qualquer violação sai com código diferente de zero. Isso é de propósito: dá
para rodar em script sem precisar ler a saída.

## Reproduzir os dados do relatório

```sh
./scripts/coletar.sh     # apaga e regenera resultados/*.csv, leva alguns minutos
python3 scripts/resumo.py
```

Os números dependem da máquina e do escalonador, então não espere bater na
vírgula. Os do relatório foram medidos num AMD Threadripper 1950X (32
threads), Linux 6.17, Go 1.23.4, com `GOMAXPROCS=8`.
