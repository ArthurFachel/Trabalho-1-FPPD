package main

import (
	"sync"
	"sync/atomic"
	"time"
)

// Item carrega a identidade de quem produziu, para a verificação final de que
// cada item foi consumido exatamente uma vez.
type Item struct {
	Produtor int
	Seq      int
	fim      bool // pílula de veneno (só na versão com semáforos)
}

// Buffer é o contrato comum das duas versões.
type Buffer interface {
	Put(Item)
	// Get bloqueia até haver item. ok=false significa que acabou de vez.
	Get() (it Item, ok bool)
	// GetTimeout é o Get com prazo d. timeout=true: nada chegou, nada saiu.
	GetTimeout(d time.Duration) (it Item, ok bool, timeout bool)
	// Fechar é chamado uma vez, depois que todos os produtores terminaram.
	Fechar(consumidores int)
	Ocupacao() int
	Nome() string
}

// Versão 1: channel de capacidade K, bloqueio pela semântica do canal.
// Encerramento por close(ch): o receive drena o que sobrou antes do ok=false.

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

// Semáforo contador: contador mais fila FIFO, como na definição da aula, não o
// atalho com channel. V() passa direto ao primeiro da fila, daí o PTimeout.

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
	// Estourou o prazo, mas um V() pode ter nos escolhido entre o disparo e
	// este Lock. Ainda na fila: saímos. Já fora: a permissão é nossa.
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

// Versão 2: buffer circular explícito, algoritmo clássico (Ben-Ari 6.8).
// mutexProd protege `in`, mutexCons protege `out`. Fim por pílula de veneno.

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
