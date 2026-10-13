package main

import (
	"sync"
	"sync/atomic"
	"time"
)

// Item carrega a identidade de quem produziu, para a verificação final de
// que cada item foi consumido exatamente uma vez.
type Item struct {
	Produtor int
	Seq      int
	fim      bool // pílula de veneno (só na versão com semáforos)
}

// Buffer é o contrato comum das duas versões.
type Buffer interface {
	Put(Item)
	// Get bloqueia até haver item. ok=false significa "acabou": não há e
	// não haverá mais itens para este consumidor.
	Get() (it Item, ok bool)
	// GetTimeout é igual ao Get, mas desiste depois de d. timeout=true
	// significa que nada chegou no prazo (e nada foi retirado).
	GetTimeout(d time.Duration) (it Item, ok bool, timeout bool)
	// Fechar é chamado uma vez, depois que todos os produtores terminaram.
	Fechar(consumidores int)
	Ocupacao() int
	Nome() string
}

// ---------------------------------------------------------------------------
// Versão 1: channel com capacidade K. O bloqueio de buffer cheio/vazio é a
// semântica do próprio channel. Encerramento: close(ch) depois que os
// produtores acabam; o receive continua entregando o que sobrou e só
// devolve ok=false quando o channel está fechado E vazio. Nada se perde.

type BufferChannel struct{ ch chan Item }

func novoBufferChannel(k int) *BufferChannel { return &BufferChannel{ch: make(chan Item, k)} }

func (b *BufferChannel) Nome() string { return "channel" }
func (b *BufferChannel) Put(it Item)  { b.ch <- it }
func (b *BufferChannel) Get() (Item, bool) {
	it, ok := <-b.ch
	return it, ok
}
func (b *BufferChannel) GetTimeout(d time.Duration) (Item, bool, bool) {
	select {
	case it, ok := <-b.ch:
		return it, ok, false
	case <-time.After(d):
		return Item{}, true, true
	}
}
func (b *BufferChannel) Fechar(int)    { close(b.ch) }
func (b *BufferChannel) Ocupacao() int { return len(b.ch) }

// ---------------------------------------------------------------------------
// Semáforo de contagem com fila FIFO de espera, construído sobre um mutex.
//
//	P(): se cont > 0, cont--. Senão entra no fim da fila e dorme.
//	V(): se há alguém na fila, acorda o primeiro e passa a permissão direto
//	     pra ele (cont não muda). Senão cont++.
//
// A passagem direta impede que uma goroutine recém-chegada "fure" a fila e
// roube a permissão de quem já esperava: a ordem de atendimento é a de
// chegada, o que elimina starvation no próprio semáforo.
//
// Cada esperador dorme num channel próprio. Isso permite a PTimeout usar
// select com time.After, que é o requisito do consumidor com timeout.

type Semaforo struct {
	mu   sync.Mutex
	cont int
	fila []chan struct{}
}

func novoSemaforo(inicial int) *Semaforo { return &Semaforo{cont: inicial} }

func (s *Semaforo) P() {
	s.mu.Lock()
	if s.cont > 0 {
		s.cont--
		s.mu.Unlock()
		return
	}
	c := make(chan struct{})
	s.fila = append(s.fila, c)
	s.mu.Unlock()
	<-c
}

// PTimeout tenta P() por no máximo d. Retorna true se obteve a permissão.
func (s *Semaforo) PTimeout(d time.Duration) bool {
	s.mu.Lock()
	if s.cont > 0 {
		s.cont--
		s.mu.Unlock()
		return true
	}
	c := make(chan struct{})
	s.fila = append(s.fila, c)
	s.mu.Unlock()

	select {
	case <-c:
		return true
	case <-time.After(d):
	}
	// Estourou o prazo. Mas um V() pode ter nos escolhido entre o
	// time.After disparar e este Lock. Se ainda estamos na fila, saímos dela
	// e ninguém nos deu nada. Se não estamos mais, a permissão já é nossa e
	// não pode ser descartada (seria um item perdido).
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, w := range s.fila {
		if w == c {
			s.fila = append(s.fila[:i], s.fila[i+1:]...)
			return false
		}
	}
	return true
}

func (s *Semaforo) V() {
	s.mu.Lock()
	if len(s.fila) > 0 {
		c := s.fila[0]
		s.fila[0] = nil
		s.fila = s.fila[1:]
		s.mu.Unlock()
		close(c)
		return
	}
	s.cont++
	s.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Versão 2: buffer circular explícito, algoritmo clássico.
//
//	Produtor:                    Consumidor:
//	  P(notFull)                   P(notEmpty)
//	  lock(mutexProd)              lock(mutexCons)
//	    buf[in] = item               item = buf[out]
//	    in = (in+1) % K              out = (out+1) % K
//	  unlock(mutexProd)            unlock(mutexCons)
//	  V(notEmpty)                  V(notFull)
//
// notFull começa em K e notEmpty em 0. Os semáforos só contam; quem
// garante que dois produtores não escrevem na mesma posição é mutexProd
// (protege `in`), e quem garante que dois consumidores não retiram o mesmo
// item é mutexCons (protege `out`). Produtor e consumidor nunca tocam a
// mesma posição ao mesmo tempo: o produtor só escreve numa posição liberada
// por V(notFull), o consumidor só lê uma posição publicada por V(notEmpty).
// Esses V/P passam por um mutex, o que dá a ordem happens-before que o
// detector de race exige.
//
// Encerramento: depois que todos os produtores terminam, Fechar insere uma
// pílula de veneno por consumidor, pelo mesmo Put. Como a fila é FIFO e
// todos os itens reais entraram antes, cada consumidor só vê uma pílula
// depois de os itens reais já terem sido retirados, e sai ao recebê-la.
// São C pílulas para C consumidores, e quem pega uma sai, então cada
// consumidor recebe exatamente uma.

type BufferSemaforo struct {
	buf       []Item
	in, out   int
	mutexProd sync.Mutex
	mutexCons sync.Mutex
	notFull   *Semaforo
	notEmpty  *Semaforo
	ocupacao  atomic.Int64 // só para métrica; não participa da sincronização
}

func novoBufferSemaforo(k int) *BufferSemaforo {
	return &BufferSemaforo{
		buf:      make([]Item, k),
		notFull:  novoSemaforo(k),
		notEmpty: novoSemaforo(0),
	}
}

func (b *BufferSemaforo) Nome() string { return "semaforo" }

func (b *BufferSemaforo) Put(it Item) {
	b.notFull.P()
	b.mutexProd.Lock()
	b.buf[b.in] = it
	b.in = (b.in + 1) % len(b.buf)
	b.mutexProd.Unlock()
	b.ocupacao.Add(1)
	b.notEmpty.V()
}

func (b *BufferSemaforo) retirar() (Item, bool) {
	b.mutexCons.Lock()
	it := b.buf[b.out]
	b.buf[b.out] = Item{}
	b.out = (b.out + 1) % len(b.buf)
	b.mutexCons.Unlock()
	b.ocupacao.Add(-1)
	b.notFull.V()
	return it, !it.fim
}

func (b *BufferSemaforo) Get() (Item, bool) {
	b.notEmpty.P()
	return b.retirar()
}

func (b *BufferSemaforo) GetTimeout(d time.Duration) (Item, bool, bool) {
	if !b.notEmpty.PTimeout(d) {
		return Item{}, true, true
	}
	it, ok := b.retirar()
	return it, ok, false
}

func (b *BufferSemaforo) Fechar(consumidores int) {
	for i := 0; i < consumidores; i++ {
		b.Put(Item{fim: true})
	}
}

func (b *BufferSemaforo) Ocupacao() int { return int(b.ocupacao.Load()) }
