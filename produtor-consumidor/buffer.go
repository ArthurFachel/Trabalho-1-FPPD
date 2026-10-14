package main

import (
	"sync"
	"sync/atomic"
	"time"
)

// Item guarda quem produziu e o número, pra conferir no fim que cada um foi
// consumido uma vez só.
type Item struct {
	Produtor int
	Seq      int
	fim      bool // pílula de veneno (só na versão com semáforos)
}

// Buffer é o que as duas versões têm em comum.
type Buffer interface {
	Put(Item)
	// Get espera até ter item. ok=false = acabou, não vem mais nada.
	Get() (it Item, ok bool)
	// GetTimeout é o Get com prazo. timeout=true = nada chegou a tempo e
	// nada foi tirado do buffer.
	GetTimeout(d time.Duration) (it Item, ok bool, timeout bool)
	// Fechar é chamado uma vez só, depois que os produtores terminaram.
	Fechar(consumidores int)
	Ocupacao() int
	Nome() string
}

// ---------------------------------------------------------------------------
// Versão 1: channel de capacidade K; cheio ou vazio, quem segura é o próprio
// channel. Pra encerrar, close(ch): o receive ainda entrega o que sobrou e só
// devolve ok=false com o channel fechado E vazio. Nada se perde.

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
// Semáforo de contagem com fila por ordem de chegada, feito em cima de um
// mutex.
//
//	P(): se cont > 0, cont--. Senão entra no fim da fila e dorme.
//	V(): se tem alguém na fila, entrega a permissão direto pro primeiro
//	     (cont não muda). Senão cont++.
//
// Entregar direto impede que um recém-chegado fure a fila. Cada um que
// espera dorme no próprio channel, e é isso que deixa o PTimeout usar
// select com time.After.

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

// PTimeout tenta o P() por no máximo d; true = conseguiu a permissão.
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
	// Estourou o prazo, mas um V() pode ter escolhido a gente entre o
	// time.After e este Lock. Se ainda estamos na fila, é só sair. Se já
	// saímos, a permissão é nossa e não dá pra jogar fora (item perdido).
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
// Versão 2: buffer circular na mão, o algoritmo clássico.
//
//	Produtor:                    Consumidor:
//	  P(notFull)                   P(notEmpty)
//	  lock(mutexProd)              lock(mutexCons)
//	    buf[in] = item               item = buf[out]
//	    in = (in+1) % K              out = (out+1) % K
//	  unlock(mutexProd)            unlock(mutexCons)
//	  V(notEmpty)                  V(notFull)
//
// notFull começa em K e notEmpty em 0; os semáforos só contam vagas e itens.
// Quem protege as posições são os mutexes: mutexProd cuida do `in`,
// mutexCons do `out`. Produtor e consumidor nunca mexem na mesma posição,
// porque um só escreve onde o V(notFull) liberou e o outro só lê o que o
// V(notEmpty) publicou.
//
// Pra encerrar, o Fechar põe uma pílula de veneno por consumidor, pelo mesmo
// Put. Como a fila é por ordem de chegada, cada consumidor só vê a pílula
// depois que os itens reais já saíram, e aí ele sai.

type BufferSemaforo struct {
	buf       []Item
	in, out   int
	mutexProd sync.Mutex
	mutexCons sync.Mutex
	notFull   *Semaforo
	notEmpty  *Semaforo
	ocupacao  atomic.Int64 // só pra medir; não faz parte da sincronização
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
