#!/usr/bin/env bash
# Regenera todos os CSVs usados no relatório. Rodar a partir de t1/.
set -euo pipefail
cd "$(dirname "$0")/.."
export GOMAXPROCS="${GOMAXPROCS:-8}"
mkdir -p bin resultados
go build -o bin/filosofos ./filosofos
go build -o bin/prodcons ./produtor-consumidor
rm -f resultados/*.csv

F=./bin/filosofos
# P1 cenário A: R fixo por filósofo
$F -n 5 -r 200 -reps 10 -q -csv resultados/fil_padrao.csv
$F -n 5 -r 200 -reps 10 -q -estrategia garcom -garcom-reserva=false -csv resultados/fil_padrao_garcom_guloso.csv
# P1 cenário B: orçamento global, comer 1ms, sem pensar
B="-n 5 -r 200 -pool -pensar 0 -pausa 0 -comer 1ms -reps 10 -q"
$F $B -estrategia hierarquia,limite,garcom -csv resultados/fil_pool_comer1ms.csv
$F $B -estrategia garcom -garcom-reserva=false -csv resultados/fil_pool_comer1ms_garcom_guloso.csv
# P1 cenário C: orçamento global, sem trabalho (contenção máxima)
C="-n 5 -r 2000 -pool -pensar 0 -pausa 0 -comer 0 -reps 10 -q"
$F $C -estrategia hierarquia,limite,garcom -csv resultados/fil_pool_sem_trabalho.csv
$F $C -estrategia garcom -garcom-reserva=false -csv resultados/fil_pool_sem_trabalho_garcom_guloso.csv
# P1: frequência de deadlock da base por cenário
for p in "2ms 1ms" "0 1ms" "2ms 0" "0 0"; do
  set -- $p
  echo "base pensar=$1 pausa=$2: $($F -estrategia base -n 5 -r 200 -reps 30 -q -pensar "$1" -pausa "$2" | grep Resumo)"
done | tee resultados/deadlock_base.txt

P=./bin/prodcons
$P -p 4 -c 4 -m 500 -k 1,10,100 -reps 5 -csv resultados/pc_equilibrado.csv > /dev/null
$P -p 4 -c 4 -m 500 -k 1,10,100 -reps 5 -produzir 1ms -consumir 1500us -csv resultados/pc_consumo_lento.csv > /dev/null
$P -p 4 -c 4 -m 20000 -k 1,10,100 -reps 5 -produzir 0 -consumir 0 -csv resultados/pc_sem_trabalho.csv > /dev/null

python3 scripts/resumo.py
