# T1: Problemas Clássicos de Concorrência

Fundamentos de Processamento Paralelo e Distribuído (98713-04)
Arthur Domingues Fachel Nunes

O código está em `filosofos/` e `produtor-consumidor/`, com instruções de compilação e execução no `README.md`. Os dados brutos estão em `resultados/` e podem ser gerados de novo com `scripts/coletar.sh`. As medições foram feitas num AMD Threadripper 1950X com Linux, Go 1.23.4 e `GOMAXPROCS=8`. As tabelas mostram médias de 10 execuções no Problema 1 e de 5 no Problema 2, cada uma com uma semente diferente. Nessa máquina, `time.Sleep` tem resolução de cerca de 1 ms, então os tempos de trabalho simulado ficam um pouco acima do configurado. Isso muda os valores absolutos, mas não a comparação entre as versões.

## Problema 1: Jantar dos Filósofos

### Solução

Cada garfo é um channel com capacidade 1 que guarda um token. Pegar o garfo é receber o token, e devolver é mandá-lo de volta. Como só existe um token por garfo, o próprio channel garante a exclusão mútua. Cada filósofo é uma goroutine que repete o ciclo pensar, pegar os garfos, comer e devolver. O filósofo `i` usa os garfos `i` (esquerdo) e `(i+1) mod N` (direito). O programa recebe N (`-n`) e R (`-r`) por parâmetro e roda as quatro versões em sequência.

- **Base:** pega o garfo esquerdo e depois o direito, segurando o primeiro enquanto espera o segundo.
- **Hierarquia de recursos:** pega primeiro o garfo de menor índice. Na prática, só o filósofo N−1 inverte a ordem e pega o garfo 0 antes do N−1.
- **Limite de filósofos à mesa:** um channel com capacidade N−1 funciona como semáforo de contagem. O filósofo envia um valor para sentar e recebe para levantar, então no máximo N−1 ficam à mesa ao mesmo tempo. Dentro da mesa, a ordem é a mesma da base.
- **Garçom:** uma goroutine central guarda o estado dos garfos. O filósofo manda um pedido e só pega os garfos depois que o garçom libera os dois. Os pedidos entram numa fila por ordem de chegada, e um pedido só é atendido se os dois garfos estiverem livres e nenhum pedido mais antigo ainda estiver esperando por eles.

Para o deadlock da base aparecer numa execução curta, há uma pausa configurável (`-pausa`, padrão 1 ms) entre pegar o primeiro e o segundo garfo. Ela não muda a lógica, só aumenta a janela em que o deadlock pode acontecer. O programa detecta o deadlock com um contador atômico dos filósofos que seguram um garfo e esperam o outro. Quando o contador chega a N, todos os garfos estão ocupados e nenhum filósofo tem os dois, então ninguém vai liberar nada.

Para cada filósofo, o programa registra o número de refeições e o tempo médio de espera, contado do pedido até estar com os dois garfos. Como todos fazem exatamente R refeições, esse número sozinho não diferencia as estratégias. Por isso existe também um modo (`-pool`) em que os filósofos disputam um total de N×R refeições, e aí a quantidade que cada um consegue passa a mostrar a fairness. A distribuição é resumida pelo índice de Jain, J = (Σx)² / (N·Σx²), que vale 1 quando todos recebem o mesmo e 1/N quando um só recebe tudo.

### Deadlock e starvation

Para haver deadlock, as quatro condições de Coffman precisam valer ao mesmo tempo: exclusão mútua, posse e espera, não preempção e espera circular. Nas quatro versões, a exclusão mútua e a não preempção continuam valendo, porque o garfo é um token único e ninguém tira o garfo de outro filósofo.

**Base.** As quatro condições valem. Se todos pegam o garfo esquerdo antes de algum conseguir o direito, forma-se o ciclo 0 → 1 → … → N−1 → 0, e o deadlock acontece. Em 30 execuções com N=5 e R=200, ele ocorreu em 14 quando o tempo de pensar era uniforme em [0, 2 ms] e nas 30 quando não havia tempo de pensar. Sem a pausa entre os garfos, não ocorreu em nenhuma das 30. Isso não quer dizer que o deadlock fique impossível sem a pausa: as condições continuam valendo, só que a janela fica pequena demais para ser observada. Quando o deadlock acontece, todos os filósofos ficam sem comer indefinidamente, o que também é starvation. Nas execuções que terminaram, a espera foi praticamente igual entre os filósofos.

**Hierarquia.** Elimina a espera circular. Como todos pegam os garfos em ordem crescente, um ciclo de espera exigiria uma sequência crescente de índices que volta ao começo, o que é impossível. Não houve deadlock em nenhuma execução. Também não há starvation em sentido estrito: não há deadlock, toda refeição dura um tempo finito e o runtime do Go atende por ordem de chegada as goroutines bloqueadas num channel. A quebra de simetria, porém, cria uma desigualdade previsível. Os filósofos 0 e N−1 disputam o garfo 0 como primeiro garfo e por isso esperam mais. O filósofo N−2 sai favorecido, porque o segundo garfo dele só é disputado pelo filósofo N−1 depois que este conseguiu o garfo 0.

**Limite de N−1 à mesa.** Também elimina a espera circular. Um ciclo de espera teria que envolver os N filósofos, mas só N−1 podem estar à mesa, e com N−1 filósofos e N garfos pelo menos um consegue os dois. Não houve deadlock. Também não há starvation: a fila de entrada da mesa é um channel, atendido por ordem de chegada, e quem está à mesa sempre avança.

**Garçom.** Elimina a posse e espera, porque o filósofo recebe os dois garfos de uma vez e nunca fica segurando um enquanto espera o outro. O garçom responde por um channel com buffer, então nunca bloqueia ao responder. Não houve deadlock. A reserva por ordem de chegada impede a starvation: um pedido só pode ser passado na frente por pedidos que não usam os mesmos garfos, e os garfos dele ficam reservados até ele ser atendido. Sem essa reserva, dois vizinhos poderiam se revezar indefinidamente e deixar sem comer o filósofo entre eles.

### Fairness

Nas duas tabelas, N=5. Na Tabela 1, cada filósofo faz R=200 refeições, com tempo de pensar uniforme em [0, 2 ms], 1 ms para comer e 1 ms de pausa entre os garfos. Na Tabela 2, os filósofos disputam 1000 refeições, sem tempo de pensar e com 1 ms para comer.

**Tabela 1.** Espera média pelos garfos, por filósofo (ms), com R fixo. A linha da base considera só as 5 execuções que não travaram.

| versão | fil. 0 | fil. 1 | fil. 2 | fil. 3 | fil. 4 | duração (ms) | deadlocks |
|---|---|---|---|---|---|---|---|
| base | 2,91 | 2,90 | 2,88 | 2,91 | 2,90 | 1116 | 5/10 |
| hierarquia | 2,32 | 1,81 | 1,56 | 1,33 | 2,32 | 1001 | 0/10 |
| limite | 2,94 | 2,92 | 2,91 | 2,92 | 2,92 | 1120 | 0/10 |
| garçom | 3,49 | 3,48 | 3,46 | 3,48 | 3,48 | 1234 | 0/10 |

**Tabela 2.** Refeições e espera média por filósofo, com 1000 refeições disputadas.

| versão | refeições (fil. 0 a 4) | espera média em ms (fil. 0 a 4) | Jain | duração (ms) |
|---|---|---|---|---|
| hierarquia | 150 / 212 / 214 / 277 / 149 | 2,60 / 1,48 / 1,46 / 0,90 / 2,61 | 0,941 | 545 |
| limite | 200 / 199 / 201 / 200 / 200 | 4,31 / 4,33 / 4,30 / 4,32 / 4,32 | 1,000 | 1082 |
| garçom | 200 / 200 / 200 / 200 / 200 | 3,53 / 3,54 / 3,54 / 3,54 / 3,55 | 1,000 | 927 |

A hierarquia foi a estratégia mais rápida e a menos justa. As duas tabelas mostram o padrão previsto na análise: os filósofos 0 e 4 esperam mais e o filósofo 3 espera menos. Quando as refeições são disputadas, o filósofo 3 come quase o dobro dos filósofos 0 e 4. Repetindo a disputa sem tempo de comer, a diferença aumenta: o filósofo 3 fez cerca de 80% das 10.000 refeições e o índice de Jain caiu para 0,31. Mesmo assim, todos comeram em todas as execuções. Então não houve starvation, só uma divisão desigual.

O limite e o garçom dividiram as refeições e o tempo de espera de forma praticamente igual (Jain = 1,000), mas a duração total foi quase o dobro da hierarquia. No limite, a posse e espera continua valendo dentro da mesa: os filósofos sentados pegam o garfo esquerdo e esperam o direito, formando uma cadeia em que só o último consegue comer. Os outros comem um de cada vez, conforme os garfos são liberados, e quase não há refeições em paralelo. O garçom teve dois comportamentos distintos: na Tabela 2, 7 execuções levaram cerca de 1088 ms e 3 levaram cerca de 550 ms, sem valores no meio. A explicação mais provável é que, em algumas execuções, a fila de pedidos fica ordenada entre vizinhos, cada pedido pendente reserva os garfos do seguinte e só um filósofo come por vez. Essa hipótese não foi confirmada.

Em resumo, a hierarquia troca fairness por vazão. O limite é justo, mas serializa as refeições. O garçom é justo e garante que ninguém fique sem comer, ao custo de um processo central e de um desempenho que variou entre as execuções.

### Encerramento ordenado

Cada filósofo termina sozinho depois de fazer as R refeições, e o programa principal espera todos com um `sync.WaitGroup`. Enquanto isso, ele faz um `select` sobre três eventos: o fim normal, o aviso do detector de deadlock e um tempo limite de segurança. Se o deadlock é detectado, o programa cancela um `context.Context`. Todas as operações que podem bloquear (pegar garfo, entrar na mesa, pedir ao garçom, pensar) também fazem `select` com `ctx.Done()`, então o filósofo que esperava o segundo garfo devolve o primeiro e termina. O garçom é encerrado por último: o programa fecha um channel de fim e espera a goroutine dele confirmar a saída. Por fim, o programa verifica se todos os garfos voltaram para a mesa e se o número de goroutines é o mesmo do início. Se alguma verificação falha, ele termina com código de erro. Cada filósofo escreve só nas próprias estatísticas, e o programa principal só as lê depois do `Wait`, o que dispensa mutex e evita data race.

## Problema 2: Produtor/Consumidor com Buffer Limitado

### Solução

As duas versões têm a mesma interface, e produtores e consumidores são iguais nas duas. Cada item leva a identificação do produtor e um número de sequência, para o programa conferir no final que cada item foi consumido exatamente uma vez. Comparar só os totais não basta, porque um item perdido e um duplicado se compensariam.

**Versão com channel.** O buffer é `make(chan Item, K)`. Com o buffer cheio, o produtor bloqueia no envio; com o buffer vazio, o consumidor bloqueia no recebimento.

**Versão com semáforos.** O buffer é um vetor circular de tamanho K, com o algoritmo visto em aula:

```
Produtor:                     Consumidor:
  P(notFull)                    P(notEmpty)
  lock(mutexProd)               lock(mutexCons)
    buf[in] = item                item = buf[out]
    in = (in+1) mod K             out = (out+1) mod K
  unlock(mutexProd)             unlock(mutexCons)
  V(notEmpty)                   V(notFull)
```

`notFull` começa em K e `notEmpty` começa em 0. Os semáforos só controlam quantas posições estão livres e quantas estão ocupadas. O `mutexProd` protege o índice `in` e impede dois produtores de escreverem na mesma posição. O `mutexCons` protege o índice `out` e impede dois consumidores de retirarem o mesmo item. Há um mutex para cada lado para que produtores e consumidores não disputem o mesmo lock. Um produtor e um consumidor também nunca mexem na mesma posição ao mesmo tempo: o produtor só escreve em posições liberadas por `V(notFull)`, e o consumidor só lê posições preenchidas antes de um `V(notEmpty)`.

O semáforo de contagem foi implementado com um mutex e uma fila de espera por ordem de chegada. Cada goroutine que espera fica bloqueada num channel próprio, e `V()` acorda a primeira da fila e passa a permissão direto para ela. Isso permite esperar com `select` e `time.After`.

**Consumidor com timeout.** Nas duas versões, o consumidor 0 espera cada item com `select` e `time.After(3 ms)`. Se nada chega no prazo, ele registra o timeout, executa uma tarefa alternativa e volta a esperar. Ele só termina quando o buffer avisa que não vêm mais itens. Na versão com semáforos há um caso delicado: o prazo pode acabar no mesmo instante em que um `V()` entrega a permissão a esse consumidor. Por isso, depois do timeout, ele confere se ainda está na fila. Se estiver, sai dela sem ter recebido nada. Se não estiver, a permissão já é dele e é usada normalmente, e nenhum item se perde.

### Deadlock e starvation

Na versão com channel, não há deadlock. Produtores só bloqueiam com o buffer cheio, e os consumidores ativos o esvaziam; consumidores só bloqueiam com o buffer vazio, e o fechamento do channel no final os libera. Na versão com semáforos, cada operação sempre espera primeiro o semáforo e só depois pega o mutex, e nenhum mutex fica preso enquanto se espera um semáforo, então não há espera circular. Se a ordem fosse invertida, um produtor poderia segurar o mutex enquanto espera `notFull` com o buffer cheio, e o sistema travaria. O outro risco está no encerramento: sem um sinal de fim, os consumidores ficariam bloqueados em `P(notEmpty)` para sempre. A solução está na seção de encerramento.

Também não há starvation. Na versão com semáforos, a fila de espera garante atendimento por ordem de chegada. Na versão com channel, o runtime do Go também atende as goroutines bloqueadas por ordem de chegada. A Tabela 3 mostra que os consumidores receberam quantidades de itens muito parecidas nas duas versões.

### Efeito do tamanho do buffer

Todas as medições usaram P=4 produtores e C=4 consumidores, com 500 itens por produtor. O tempo para produzir e para consumir cada item segue uma exponencial com média de 1 ms. Em todas as execuções, o total produzido foi igual ao consumido, sem item perdido ou duplicado.

**Tabela 3.** Efeito de K com trabalho simulado.

| versão | K | throughput (itens/s) | ocupação média | itens por consumidor | bloqueio por produtor (ms) | espera por consumidor (ms) |
|---|---|---|---|---|---|---|
| channel | 1 | 2259 | 0,50 | 485 / 502 / 511 / 502 | 95,1 | 107,7 |
| semáforos | 1 | 2244 | 0,50 | 489 / 502 / 509 / 501 | 94,3 | 109,0 |
| channel | 10 | 2416 | 5,06 | 486 / 501 / 509 / 503 | 29,9 | 48,3 |
| semáforos | 10 | 2439 | 5,02 | 490 / 503 / 507 / 501 | 32,5 | 42,0 |
| channel | 100 | 2522 | 31,9 | 488 / 500 / 507 / 505 | 0,1 | 14,0 |
| semáforos | 100 | 2530 | 30,3 | 489 / 501 / 510 / 500 | 0,1 | 16,8 |

Com K=1, produtores e consumidores ficam praticamente sincronizados: um produtor só avança quando algum consumidor retira o item anterior, e qualquer variação no ritmo de um lado vira espera no outro. Buffers maiores absorvem essas variações. De K=1 para K=100, o tempo que cada produtor passa bloqueado cai de cerca de 95 ms para 0,1 ms, a espera dos consumidores cai de cerca de 108 ms para cerca de 15 ms e o throughput sobe cerca de 12%. Com K=100, o desempenho já está perto do limite dado pelo ritmo dos produtores, então aumentar mais o buffer traria pouco ganho. A ocupação média com K=100 variou bastante entre as execuções (31,9 ± 18,9 na versão com channel). Como produtores e consumidores têm o mesmo ritmo médio, o buffer não tende a encher nem a esvaziar. Ele só absorve as diferenças momentâneas entre os dois lados, e o nível em que se estabiliza muda de uma execução para outra.

Com trabalho simulado, as duas versões tiveram desempenho equivalente, com diferenças dentro da variação entre execuções, porque o custo da sincronização é pequeno perto do tempo de produzir e consumir cada item. Para comparar só o custo da sincronização, os experimentos foram repetidos sem trabalho simulado, com 20.000 itens por produtor.

**Tabela 4.** Comparação sem trabalho simulado.

| versão | K | throughput (itens/s) | ocupação média | itens por consumidor |
|---|---|---|---|---|
| channel | 1 | 749.580 | 0,54 | 14181 / 21825 / 22173 / 21821 |
| semáforos | 1 | 538.111 | 0,47 | 20014 / 19993 / 19989 / 20005 |
| channel | 10 | 775.811 | 5,29 | 14060 / 21701 / 22220 / 22019 |
| semáforos | 10 | 685.788 | 4,97 | 22518 / 19233 / 19329 / 18920 |
| channel | 100 | 711.950 | 60,1 | 16381 / 21314 / 21184 / 21121 |
| semáforos | 100 | 1.110.398 | 73,5 | 20183 / 20400 / 19714 / 19702 |

Com K=1, o channel foi 39% mais rápido. Como o buffer fica quase sempre cheio ou vazio, a versão com semáforos bloqueia em quase toda operação, e cada bloqueio passa pela fila do semáforo. Já o channel, quando há um consumidor esperando, entrega o item direto para ele. Com K=100 a situação se inverte, e a versão com semáforos foi 56% mais rápida. Com o buffer folgado, os semáforos quase nunca bloqueiam e produtores e consumidores usam mutexes diferentes, enquanto no channel os dois lados disputam o mesmo lock interno do runtime. Sem trabalho simulado, o consumidor 0 da versão com channel recebeu menos itens, porque cria um timer a cada tentativa de recebimento. Na versão com semáforos, a fila de espera divide os itens por ordem de chegada. Com trabalho simulado, essa diferença desaparece (Tabela 3).

### Encerramento ordenado

As duas versões seguem os mesmos passos: o programa espera todos os produtores terminarem com um `WaitGroup`, sinaliza o fim no buffer e espera os consumidores esvaziarem o buffer e terminarem. Depois confere item por item que nada foi perdido nem duplicado, compara o total produzido com o consumido e verifica se o número de goroutines voltou ao valor inicial.

Na versão com channel, o sinal de fim é `close(ch)`. Fechar o channel não descarta os itens que ainda estão nele: o consumidor continua recebendo normalmente e só recebe `ok == false` quando o channel está fechado e vazio. Como o fechamento só acontece depois que todos os produtores terminaram, ninguém envia para um channel fechado.

Na versão com semáforos, o sinal de fim são C itens especiais de término (pílulas de veneno), colocados pela mesma operação `Put`. O buffer é lido em ordem e todos os itens reais entraram antes das pílulas, então uma pílula só é retirada depois de todos os itens reais. Cada consumidor termina ao receber uma pílula, e assim cada um recebe exatamente uma das C pílulas.

### Verificação

Os dois programas rodam sem erros com `go run -race` e `go test -race`. Os testes incluem execuções sem trabalho simulado, para aumentar ao máximo a disputa entre as goroutines, com vários valores de K, P e C, e uma execução com timeout de 1 µs, para forçar a corrida entre o timeout e o `V()`.

## Uso de ferramentas de IA

O assistente Claude (Anthropic) foi usado para escrever o código, os testes e os scripts de coleta, rodar os experimentos e redigir uma primeira versão deste relatório. Todos os números apresentados vêm de execuções registradas em `resultados/`, que podem ser reproduzidas com `scripts/coletar.sh`.

[Completar: o que foi revisado, alterado e verificado pelo autor, e como a ferramenta foi usada nas decisões de projeto.]
