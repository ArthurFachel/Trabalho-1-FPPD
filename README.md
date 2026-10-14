# T1 — Problemas Clássicos de Concorrência (FPPD 98713-04)

Arthur Domingues Fachel Nunes

São dois programas Go independentes: o Jantar dos Filósofos e o
produtor/consumidor com buffer limitado. Só biblioteca padrão, nenhuma
dependência externa.

```
t1/
├── go.mod
├── filosofos/              Problema 1: Jantar dos Filósofos
│   ├── mesa.go             garfos (channels), detector de deadlock, 4 estratégias
│   ├── main.go             CLI, coleta de métricas, CSV, verificação de encerramento
│   └── mesa_test.go
├── produtor-consumidor/    Problema 2: buffer limitado
│   ├── buffer.go           versão channel, semáforo FIFO, versão com semáforos
│   ├── main.go             CLI, consumidor com timeout, verificação item a item, CSV
│   └── buffer_test.go
├── scripts/
│   ├── coletar.sh          reproduz todos os CSVs de resultados/
│   └── resumo.py           tabelas (média ± desvio) a partir dos CSVs, só stdlib
├── resultados/             CSVs brutos + resumo.txt
└── relatorio/              relatorio.md (o PDF ainda não foi gerado)
```

## O que precisa ter instalado

Go 1.22 ou mais novo, porque o código usa `math/rand/v2` e `for` com variável
por iteração. O `-race` depende de cgo, então precisa de um `gcc` no PATH.
Testei com Go 1.23.4 e 1.27.2 em linux/amd64.

## Compilar e rodar

```sh
cd t1
go build ./...

# Problema 1: N filósofos, R refeições cada, as 4 versões em sequência
go run -race ./filosofos -n 5 -r 100

# só uma estratégia
go run -race ./filosofos -n 5 -r 100 -estrategia hierarquia

# forçar o deadlock da versão base (sem pensar, com pausa entre os garfos)
go run -race ./filosofos -n 5 -r 100 -estrategia base -pensar 0 -pausa 2ms

# Problema 2: P produtores, C consumidores, K = 1, 10 e 100, as 2 versões
go run -race ./produtor-consumidor -p 4 -c 4 -m 500 -k 1,10,100

# ver o consumidor 0 caindo no timeout (produção lenta)
go run -race ./produtor-consumidor -p 1 -c 2 -m 30 -k 5 -produzir 5ms -timeout 2ms -v

# testes, incluindo os casos adversariais
go test -race ./...
```

## Flags

`filosofos`

| flag | padrão | o que faz |
|---|---|---|
| `-n` | 5 | número de filósofos |
| `-r` | 100 | refeições por filósofo |
| `-pool` | false | em vez de R por filósofo, um total de N*R refeições disputado por todos. É esse modo que faz a contagem de refeições medir fairness |
| `-estrategia` | todas | `base`, `hierarquia`, `limite`, `garcom` ou lista separada por vírgula |
| `-pensar` | 2ms | tempo de pensar, uniforme em [0, pensar] |
| `-comer` | 1ms | tempo de comer |
| `-pausa` | 1ms | pausa entre pegar o 1º e o 2º garfo |
| `-garcom-reserva` | true | `false` desliga a reserva FIFO do garçom, pra comparar com a variante gulosa |
| `-reps`, `-seed`, `-csv`, `-q` | | repetições, semente, saída CSV, modo silencioso |

`produtor-consumidor`

| flag | padrão | o que faz |
|---|---|---|
| `-p`, `-c` | 4, 4 | produtores e consumidores. O consumidor 0 é o que usa `select` + `time.After` |
| `-k` | 1,10,100 | capacidades do buffer a testar |
| `-m` | 500 | itens por produtor |
| `-produzir`, `-consumir` | 1ms, 1ms | média (exponencial) do trabalho por item, 0 desliga |
| `-timeout` | 3ms | prazo do consumidor 0 |
| `-versao` | todas | `channel`, `semaforo` |
| `-reps`, `-seed`, `-csv`, `-v` | | |

## Como saber se deu certo

Os dois programas só saem com código 0 se todas as goroutines que criaram já
terminaram, o que é conferido contra o `runtime.NumGoroutine()` do começo. O
dos filósofos ainda checa se todos os garfos voltaram pra mesa, e o do
produtor/consumidor checa se cada item produzido foi consumido uma vez só. Se
alguma dessas verificações falhar, o código de saída é diferente de 0.

## Refazer os dados do relatório

```sh
./scripts/coletar.sh     # apaga e regenera resultados/*.csv (leva alguns minutos)
python3 scripts/resumo.py
```

Os números mudam de máquina pra máquina e dependem do escalonador. Os do
relatório saíram de um AMD Threadripper 1950X (32 threads), Linux 6.17,
Go 1.23.4 e `GOMAXPROCS=8`.
