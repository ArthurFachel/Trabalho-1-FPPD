# T1: Problemas Clássicos de Concorrência

Fundamentos de Processamento Paralelo e Distribuído (98713-04)
Arthur Domingues Fachel Nunes

O código está em `filosofos/` e `produtor-consumidor/`. O `README.md` tem as
instruções de compilação e as flags. Os dados brutos ficam em `resultados/` e
podem ser gerados de novo com `scripts/coletar.sh`.

Rodei tudo num AMD Threadripper 1950X com Linux, Go 1.23.4 e `GOMAXPROCS=8`.
As tabelas são médias de 10 execuções no Problema 1 e de 5 no Problema 2,
cada uma com uma semente diferente. Vale um aviso sobre os números absolutos:
nessa máquina o `time.Sleep` tem resolução de uns 1 ms, então os tempos de
trabalho simulado saem sempre um pouco acima do que configurei. Isso desloca
os valores, mas não muda a comparação entre as versões, que é o que interessa.

## Problema 1: Jantar dos Filósofos

### Como resolvi

Cada garfo é um channel de capacidade 1 guardando um token. Pegar o garfo é
receber o token, devolver é mandá-lo de volta. Como só existe um token por
garfo, a exclusão mútua sai de graça do próprio channel, sem mutex nenhum.
É a ideia do canal booleano de buffer 1 que vimos em aula, na forma
idiomática com `chan struct{}`.

Vale registrar uma diferença em relação ao Algoritmo 8.5 do Ben-Ari: lá cada
garfo é um processo separado, que oferece o token e recebe de volta num canal
síncrono. Aqui o garfo é só o canal com o token, sem goroutine própria. O
efeito é o mesmo e economiza N goroutines.

Cada filósofo é uma goroutine no ciclo pensar, pegar os garfos, comer,
devolver. O filósofo `i` usa os garfos `i` e `(i+1) mod N`. N vem de `-n` e R
de `-r`, e o programa roda as quatro versões em sequência.

- **Base.** Pega o esquerdo e depois o direito, segurando o primeiro enquanto
  espera o segundo.
- **Hierarquia.** Pega sempre o garfo de menor índice primeiro. Na prática só
  o filósofo N-1 muda de comportamento: ele pega o garfo 0 antes do N-1.
- **Limite de lugares.** Um channel de capacidade N-1 serve de semáforo
  contador. O filósofo envia para sentar e recebe para levantar, então no
  máximo N-1 ficam à mesa. Lá dentro, a ordem é a mesma da base.
- **Garçom.** Uma goroutine central é dona do estado dos garfos. O filósofo
  manda o pedido e só toca nos garfos depois que o garçom libera os dois. Os
  pedidos entram numa fila por ordem de chegada, e um pedido só é atendido se
  os dois garfos estão livres e nenhum pedido mais antigo está esperando por
  eles.

Para o deadlock da base aparecer numa execução curta, coloquei uma pausa
configurável entre pegar o primeiro e o segundo garfo (`-pausa`, 1 ms por
padrão). Ela não muda a lógica, só alarga a janela. A detecção é um contador
atômico dos filósofos que seguram um garfo e esperam o outro: quando chega a
N, os N garfos estão ocupados e ninguém tem dois, então ninguém vai liberar
nada. É certeza de deadlock, não suspeita por timeout.

Sobre as métricas: o programa registra refeições e tempo médio de espera por
filósofo, contado do pedido até estar com os dois garfos. Só que, com R fixo,
todo mundo faz exatamente R refeições e esse número não diferencia nada. Por
isso existe o modo `-pool`, em que os filósofos disputam um total de N*R
refeições. Aí a quantidade que cada um consegue vira medida de fairness.
Resumo a distribuição pelo índice de Jain, J = (soma de x)^2 / (N * soma de
x^2), que dá 1 quando todos recebem igual e 1/N quando um leva tudo.

### Deadlock e starvation

Deadlock exige as quatro condições de Coffman ao mesmo tempo. Nas quatro
versões, exclusão mútua e não preempção continuam valendo: o garfo é um token
único e ninguém arranca garfo da mão de outro. Então a análise se resume a
hold-and-wait e espera circular.

**Base.** As quatro valem. Se todos pegam o esquerdo antes de alguém
conseguir o direito, fecha o ciclo 0 -> 1 -> ... -> N-1 -> 0. Em 30 execuções
com N=5 e R=200, travou em 14 com o tempo de pensar em [0, 2 ms] e nas 30 sem
tempo de pensar. Sem a pausa entre os garfos não travou nenhuma vez, o que
não quer dizer que fique impossível: as condições continuam todas lá, a
janela é que fica pequena demais para eu observar. Quando trava, ninguém mais
come, o que também é starvation.

**Hierarquia.** Elimina a espera circular. Como todos pegam em ordem
crescente de índice, um ciclo exigiria uma sequência crescente que volta ao
começo, e isso não existe. Nenhum deadlock em nenhuma execução. Starvation
em sentido estrito também não: sem deadlock, cada refeição dura um tempo
finito e o runtime do Go atende por ordem de chegada quem está bloqueado num
channel. Mas a quebra de simetria cria uma desigualdade previsível. Os
filósofos 0 e N-1 disputam o garfo 0 como primeiro garfo e esperam mais. O
N-2 sai favorecido, porque o segundo garfo dele só é disputado pelo N-1
depois que este já conseguiu o garfo 0.

**Limite de N-1 à mesa.** Aqui preciso ser explícito sobre uma diferença em
relação ao material de aula. O slide da Unidade 3 atribui a essa estratégia a
quebra da hold-and-wait. Pela minha implementação, o que cai é a espera
circular: um ciclo teria que envolver os N filósofos, mas só N-1 sentam, e
com N-1 filósofos e N garfos pelo menos um consegue os dois. A hold-and-wait
continua valendo dentro da mesa, porque o filósofo sentado pega o esquerdo e
fica esperando o direito. Os dados da seção seguinte sustentam essa leitura:
é justamente essa posse e espera residual que serializa as refeições e dobra
a duração. Não houve deadlock, e também não há starvation, porque a fila de
entrada é um channel atendido por ordem de chegada e quem está à mesa sempre
avança.

**Garçom.** Esse sim elimina a hold-and-wait, porque o filósofo recebe os
dois garfos de uma vez e nunca segura um esperando o outro. O garçom responde
por um channel com buffer, então nunca bloqueia ao responder. Nenhum
deadlock. A reserva por ordem de chegada é o que impede starvation: um pedido
só é ultrapassado por pedidos que não usam os mesmos garfos, e os garfos dele
ficam reservados até ser atendido.

Para mostrar que essa reserva não é enfeite, implementei a variante gulosa
(`-garcom-reserva=false`), que atende qualquer pedido cujos garfos estejam
livres, sem reservar nada para quem espera há mais tempo. No pool de 1000
refeições, ela produziu o pior caso de toda a bateria: um filósofo comeu 1
vez enquanto outro comeu 250, com espera máxima de 539 ms contra 4,7 ms do
garçom com reserva. Isso é starvation observada, não só argumentada. Dois
vizinhos se revezam e deixam o filósofo do meio sem comer.

### Fairness

Nas duas tabelas, N=5. Na Tabela 1 cada filósofo faz R=200 refeições, com
tempo de pensar em [0, 2 ms], 1 ms para comer e 1 ms de pausa entre os
garfos. Na Tabela 2 eles disputam 1000 refeições, sem pensar e com 1 ms para
comer.

**Tabela 1.** Espera média pelos garfos, por filósofo (ms), com R fixo. A
linha da base considera só as 5 execuções que não travaram.

| versão | fil. 0 | fil. 1 | fil. 2 | fil. 3 | fil. 4 | duração (ms) | deadlocks |
|---|---|---|---|---|---|---|---|
| base | 2,91 | 2,90 | 2,88 | 2,91 | 2,90 | 1116 | 5/10 |
| hierarquia | 2,32 | 1,81 | 1,56 | 1,33 | 2,32 | 1001 | 0/10 |
| limite | 2,94 | 2,92 | 2,91 | 2,92 | 2,92 | 1120 | 0/10 |
| garçom | 3,49 | 3,48 | 3,46 | 3,48 | 3,48 | 1234 | 0/10 |

**Tabela 2.** Refeições e espera média por filósofo, com 1000 refeições
disputadas.

| versão | refeições (fil. 0 a 4) | espera média em ms (fil. 0 a 4) | Jain | duração (ms) |
|---|---|---|---|---|
| hierarquia | 150 / 212 / 214 / 277 / 149 | 2,60 / 1,48 / 1,46 / 0,90 / 2,61 | 0,941 | 545 |
| limite | 200 / 199 / 201 / 200 / 200 | 4,31 / 4,33 / 4,30 / 4,32 / 4,32 | 1,000 | 1082 |
| garçom | 200 / 200 / 200 / 200 / 200 | 3,53 / 3,54 / 3,54 / 3,54 / 3,55 | 1,000 | 927 |
| garçom guloso | 1 / 250 / 249 / 250 / 250 | 538,9 / 0,6 / 0,6 / 0,6 / 0,6 | 0,950 | 548 |

A hierarquia foi a mais rápida e a menos justa. As duas tabelas mostram
exatamente o padrão que a análise previa: 0 e 4 esperam mais, 3 espera menos.
Na disputa, o filósofo 3 come quase o dobro de 0 e 4. Refazendo a disputa sem
tempo de comer, que é o cenário de máxima contenção, a diferença explode: o
filósofo 3 fez cerca de 80% das 10.000 refeições e o Jain caiu para 0,31.
Ainda assim todos comeram em todas as execuções, então não é starvation. É
divisão desigual.

O limite e o garçom dividiram refeições e espera de forma praticamente igual
(Jain = 1,000), mas levaram quase o dobro do tempo da hierarquia. No limite,
como argumentei acima, a posse e espera sobrevive dentro da mesa: os sentados
pegam o esquerdo e esperam o direito, formando uma cadeia em que só o último
consegue comer. Os outros vão comendo um de cada vez, conforme os garfos
voltam, e quase não há refeições em paralelo.

O garçom teve um comportamento que não consegui explicar por completo. Na
Tabela 2, 7 execuções levaram perto de 1088 ms e 3 levaram perto de 550 ms,
sem nenhum valor no meio. A hipótese que me parece mais provável é que em
algumas execuções a fila de pedidos fica ordenada entre vizinhos, cada pedido
pendente reserva os garfos do seguinte e só um filósofo come por vez. Não
consegui confirmar isso com os dados que coletei, e prefiro deixar registrado
como hipótese a inventar uma explicação fechada.

Resumindo o trade-off: a hierarquia troca fairness por vazão. O limite é
justo mas serializa. O garçom com reserva é justo e garante que ninguém fique
sem comer, ao custo de um processo central e de uma duração que variou entre
execuções. O garçom sem reserva mostra que é a política de fila, e não a
arbitragem central em si, que compra a garantia contra starvation.

### Encerramento ordenado

Cada filósofo termina sozinho depois das R refeições, e o `main` espera todos
num `sync.WaitGroup`. Enquanto espera, ele faz `select` sobre três eventos: o
fim normal, o aviso do detector de deadlock e um tempo limite de segurança.
Se o deadlock dispara, o programa cancela um `context.Context`. Todas as
operações que podem bloquear (pegar garfo, sentar, pedir ao garçom, pensar)
também fazem `select` com `ctx.Done()`, então quem estava preso no segundo
garfo devolve o primeiro e sai. O garçom é encerrado por último: o programa
fecha o channel de fim e espera a goroutine dele confirmar a saída.

No final, duas conferências: todos os garfos voltaram para a mesa e o número
de goroutines é o mesmo do início. Qualquer falha dá código de saída
diferente de zero. Sobre data race, cada filósofo escreve só nas próprias
estatísticas e o `main` só lê depois do `Wait`, que já estabelece a relação
happens-before. Não precisa de mutex para isso.

## Problema 2: Produtor/Consumidor com Buffer Limitado

### Como resolvi

As duas versões têm a mesma interface, e produtores e consumidores são
idênticos nas duas. Cada item carrega o id do produtor e um número de
sequência, para eu conferir no final que cada item foi consumido exatamente
uma vez. Comparar só os totais não bastaria: um item perdido e um duplicado
se cancelariam e o teste passaria sem motivo.

**Versão com channel.** O buffer é `make(chan Item, K)`. Buffer cheio bloqueia
o envio, buffer vazio bloqueia o recebimento. É a semântica do canal
assíncrono, em que o próprio canal concentra o papel dos dois semáforos.

**Versão com semáforos.** Vetor circular de tamanho K com o algoritmo visto em
aula (Ben-Ari, 6.8):

```
Produtor:                     Consumidor:
  P(notFull)                    P(notEmpty)
  lock(mutexProd)               lock(mutexCons)
    buf[in] = item                item = buf[out]
    in = (in+1) mod K             out = (out+1) mod K
  unlock(mutexProd)             unlock(mutexCons)
  V(notEmpty)                   V(notFull)
```

`notFull` começa em K e `notEmpty` em 0. Os semáforos só contam vagas e itens.
Quem impede dois produtores de escreverem na mesma posição é o `mutexProd`,
que protege o `in`, e quem impede dois consumidores de pegarem o mesmo item é
o `mutexCons`, que protege o `out`. Usei um mutex de cada lado para que
produtores e consumidores não disputem o mesmo lock. Produtor e consumidor
também nunca tocam a mesma posição ao mesmo tempo: o produtor só escreve onde
um `V(notFull)` liberou, o consumidor só lê o que um `V(notEmpty)` publicou.

Uma decisão que merece explicação. O slide mostra o atalho de simular
semáforo contador com um canal de buffer N. Não usei esse atalho, porque com
ele a versão 2 viraria uma versão 1 disfarçada e a comparação perderia a
graça. Implementei o semáforo como a própria definição da aula: contador mais
fila de esperadores, com mutex garantindo a atomicidade do par. Cada
esperador dorme num channel próprio, e o `V()` acorda o primeiro da fila e
passa a permissão direto para ele, sem mexer no contador. Isso faz dele um
semáforo forte, com atendimento FIFO, e é o que permite esperar com `select`
e `time.After`.

**Consumidor com timeout.** Nas duas versões, o consumidor 0 espera cada item
com `select` e `time.After(3 ms)`. Estourou o prazo, ele registra o timeout,
faz uma tarefa alternativa e volta a esperar. Ele nunca abandona o buffer por
timeout, só sai quando o buffer avisa que acabou, senão itens ficariam sem
consumidor. Na versão com semáforos existe um caso de corrida delicado: o
prazo pode vencer no mesmo instante em que um `V()` entrega a permissão a
esse consumidor. Por isso, depois do timeout ele reconfere se ainda está na
fila. Se estiver, sai dela e não recebeu nada. Se não estiver, a permissão já
é dele e precisa ser usada, porque descartar significaria perder um item.

### Deadlock e starvation

Na versão com channel não há deadlock. Produtores só bloqueiam com o buffer
cheio, e os consumidores ativos esvaziam; consumidores só bloqueiam com o
buffer vazio, e o fechamento do channel libera todos no final.

Na versão com semáforos, a ordem das operações é o que salva: sempre se
espera o semáforo primeiro e só depois se pega o mutex, e nenhum mutex fica
preso enquanto se espera semáforo. Sem mutex retido durante a espera, não há
espera circular. Invertendo a ordem o programa travaria: um produtor
seguraria o `mutexProd` esperando `notFull` com o buffer cheio, e nenhum
consumidor conseguiria avançar. O outro risco é o encerramento, porque sem um
sinal de fim os consumidores ficariam em `P(notEmpty)` para sempre. Isso está
resolvido na seção de encerramento.

Starvation também não ocorre. Na versão com semáforos, a fila garante
atendimento por ordem de chegada. Na versão com channel, o runtime do Go
também atende as goroutines bloqueadas em ordem de chegada. A Tabela 3 mostra
os consumidores recebendo quantidades bem parecidas nas duas versões.

### Efeito do tamanho do buffer

Todas as medições com P=4, C=4 e 500 itens por produtor. O tempo de produzir
e de consumir cada item segue uma exponencial de média 1 ms. Em todas as
execuções o total produzido bateu com o consumido, sem item perdido nem
duplicado.

**Tabela 3.** Efeito de K com trabalho simulado.

| versão | K | throughput (itens/s) | ocupação média | itens por consumidor | bloqueio por produtor (ms) | espera por consumidor (ms) |
|---|---|---|---|---|---|---|
| channel | 1 | 2259 | 0,50 | 485 / 502 / 511 / 502 | 95,1 | 107,7 |
| semáforos | 1 | 2244 | 0,50 | 489 / 502 / 509 / 501 | 94,3 | 109,0 |
| channel | 10 | 2416 | 5,06 | 486 / 501 / 509 / 503 | 29,9 | 48,3 |
| semáforos | 10 | 2439 | 5,02 | 490 / 503 / 507 / 501 | 32,5 | 42,0 |
| channel | 100 | 2522 | 31,9 | 488 / 500 / 507 / 505 | 0,1 | 14,0 |
| semáforos | 100 | 2530 | 30,3 | 489 / 501 / 510 / 500 | 0,1 | 16,8 |

Com K=1 os dois lados ficam praticamente sincronizados: o produtor só avança
quando alguém retira o item anterior, e qualquer variação de ritmo de um lado
vira espera no outro. É o acoplamento máximo. Buffers maiores absorvem essas
variações. De K=1 para K=100, o tempo bloqueado por produtor cai de uns 95 ms
para 0,1 ms, a espera dos consumidores cai de uns 108 ms para uns 15 ms e o
throughput sobe uns 12%. Com K=100 o sistema já está perto do limite imposto
pelo ritmo dos produtores, então aumentar mais o buffer renderia pouco.

Um detalhe que me chamou atenção: a ocupação média com K=100 variou muito
entre execuções (31,9 ± 18,9 na versão com channel). Faz sentido. Como os
dois lados têm o mesmo ritmo médio, o buffer não tende a encher nem a
esvaziar, ele só absorve as diferenças momentâneas, e o nível onde isso se
estabiliza muda de execução para execução.

Com trabalho simulado, as duas versões empataram, com diferenças dentro da
variação entre execuções. Era esperado: o custo da sincronização é pequeno
perto do 1 ms de produzir e consumir cada item. Para isolar só esse custo,
repeti os experimentos sem trabalho simulado, com 20.000 itens por produtor.

**Tabela 4.** Comparação sem trabalho simulado.

| versão | K | throughput (itens/s) | ocupação média | itens por consumidor |
|---|---|---|---|---|
| channel | 1 | 749.580 | 0,54 | 14181 / 21825 / 22173 / 21821 |
| semáforos | 1 | 538.111 | 0,47 | 20014 / 19993 / 19989 / 20005 |
| channel | 10 | 775.811 | 5,29 | 14060 / 21701 / 22220 / 22019 |
| semáforos | 10 | 685.788 | 4,97 | 22518 / 19233 / 19329 / 18920 |
| channel | 100 | 711.950 | 60,1 | 16381 / 21314 / 21184 / 21121 |
| semáforos | 100 | 1.110.398 | 73,5 | 20183 / 20400 / 19714 / 19702 |

Com K=1 o channel foi 39% mais rápido. O buffer vive cheio ou vazio, então a
versão com semáforos bloqueia em quase toda operação e cada bloqueio passa
pela fila. O channel, quando já tem um consumidor esperando, entrega o item
direto para ele e pula o buffer.

Com K=100 a coisa se inverte e os semáforos ficam 56% na frente. Com o buffer
folgado eles quase nunca bloqueiam, e produtores e consumidores usam mutexes
diferentes. No channel, os dois lados continuam disputando o mesmo lock
interno do runtime, que vira o gargalo.

Ainda na Tabela 4, o consumidor 0 da versão com channel recebeu bem menos
itens que os outros. A causa é o `time.After`, que cria um timer novo a cada
tentativa de recebimento, e sem trabalho simulado esse custo pesa. Na versão
com semáforos a fila divide por ordem de chegada e isso não aparece. Com
trabalho simulado a diferença some (Tabela 3).

### Encerramento ordenado

As duas versões seguem os mesmos três passos: espera todos os produtores num
`WaitGroup`, sinaliza o fim no buffer, espera os consumidores drenarem e
saírem. Depois confere item por item que nada se perdeu nem duplicou, compara
os totais e verifica se o número de goroutines voltou ao valor inicial.

Na versão com channel o sinal de fim é `close(ch)`. Fechar não descarta o que
ainda está dentro: o consumidor continua recebendo normalmente e só vê
`ok == false` quando o channel está fechado e vazio. Como o fechamento só
acontece depois que todos os produtores terminaram, ninguém envia para canal
fechado.

Na versão com semáforos uso C pílulas de veneno, inseridas pelo mesmo `Put`.
Como o buffer é FIFO e todos os itens reais entraram antes, uma pílula só é
retirada depois que os itens reais já saíram. Cada consumidor sai ao receber
uma, e como são C pílulas para C consumidores, cada um recebe exatamente uma.

### Verificação

Os dois programas rodam sem erro com `go run -race` e `go test -race`. Os
testes incluem execuções sem trabalho simulado, para maximizar a disputa
entre as goroutines, com vários valores de K, P e C. Tem também um caso com
timeout de 1 microssegundo, feito de propósito para forçar a corrida entre o
timeout e o `V()` que descrevi na seção da solução.

## Uso de ferramentas de IA

Usei o assistente Claude (Anthropic) ao longo do trabalho, nas seguintes
etapas:

- **Projeto da solução.** Discuti as alternativas de detecção de deadlock e a
  política de atendimento do garçom. A decisão de usar contador atômico em
  vez de timeout, e a de implementar a reserva FIFO, saíram dessa conversa.
- **Geração de código.** A primeira versão dos arquivos Go, dos testes e dos
  scripts de coleta foi gerada pela ferramenta a partir das minhas
  especificações.
- **Depuração.** A corrida entre o `time.After` e o `V()` no `PTimeout` foi
  identificada e corrigida com apoio da ferramenta.
- **Análise dos dados.** Usei a ferramenta para montar o script de agregação
  dos CSVs e para discutir as hipóteses sobre o comportamento bimodal do
  garçom.
- **Redação.** A primeira versão deste relatório foi redigida com a
  ferramenta e depois revisada e reescrita por mim.

Todos os números apresentados vêm de execuções registradas em `resultados/` e
podem ser reproduzidos com `scripts/coletar.sh`. 

[Arthur: substitua este parágrafo por uma descrição do que você revisou,
alterou e verificou por conta própria, e de quais decisões de projeto foram
suas. Essa parte eu não posso escrever por você.]

## Referências

- BEN-ARI, M. *Principles of Concurrent and Distributed Programming*. 2. ed.
  Addison-Wesley, 2006. Algoritmos 6.8 (produtor-consumidor com semáforos),
  6.10 e 8.5 (jantar dos filósofos), base das duas implementações.
- DIJKSTRA, E. W. *Hierarchical Ordering of Sequential Processes*. Acta
  Informatica, v. 1, n. 2, 1971. Formulação original do jantar dos filósofos.
- COFFMAN, E. G.; ELPHICK, M.; SHOSHANI, A. *System Deadlocks*. ACM Computing
  Surveys, v. 3, n. 2, 1971. As quatro condições usadas na análise.
- JAIN, R.; CHIU, D.; HAWE, W. *A Quantitative Measure of Fairness and
  Discrimination for Resource Allocation in Shared Computer Systems*. DEC
  Research Report TR-301, 1984. Índice de fairness usado nas Tabelas 2 e 4.
- Material da disciplina FPPD, Unidades 2 e 3 (slides de conceitos de
  concorrência, programação concorrente em Go, canais de comunicação e
  mecanismos de sincronização).
- Documentação da linguagem Go: pacotes `sync`, `sync/atomic`, `context` e
  `time`, e o *The Go Memory Model* (go.dev/ref/mem), usado para justificar a
  ausência de mutex nas estatísticas por filósofo.
