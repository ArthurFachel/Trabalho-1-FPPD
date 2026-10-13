package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Garfo é um channel com capacidade 1. O garfo está livre quando o channel
// contém o token; pegar = receber o token, devolver = enviar o token de volta.
// Como só existe um token por garfo, a exclusão mútua vem do próprio channel.
type Garfo chan struct{}

func novoGarfo() Garfo {
	g := make(Garfo, 1)
	g <- struct{}{}
	return g
}

// Mesa guarda os garfos e o detector de deadlock, que é comum a todas as
// estratégias.
//
// Detector: esperandoSegundo conta quantos filósofos seguram exatamente um
// garfo e estão esperando o segundo. Se esse número chega a N, os N garfos
// estão todos na mão de alguém, ninguém tem dois, e portanto ninguém vai
// devolver nada: é deadlock com certeza (espera circular fechada), não uma
// suspeita por timeout. O contador nunca passa de N por engano: um filósofo
// que já pegou o segundo garfo mas ainda não decrementou seguraria 2 garfos,
// e N filósofos contados exigiriam N+1 garfos.
type Mesa struct {
	n      int
	garfos []Garfo
	pausa  time.Duration // tempo entre pegar o 1o e o 2o garfo

	esperandoSegundo atomic.Int32
	deadlock         chan struct{} // fechado quando o deadlock é detectado
	onceDeadlock     sync.Once
}

func novaMesa(n int, pausa time.Duration) *Mesa {
	m := &Mesa{n: n, pausa: pausa, deadlock: make(chan struct{})}
	m.garfos = make([]Garfo, n)
	for i := range m.garfos {
		m.garfos[i] = novoGarfo()
	}
	return m
}

func (m *Mesa) esquerdo(i int) int { return i }
func (m *Mesa) direito(i int) int  { return (i + 1) % m.n }

// dormir respeita o cancelamento: devolve false se o contexto acabou.
func dormir(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// pegarEmOrdem pega o garfo `primeiro` e depois o `segundo`, segurando o
// primeiro enquanto espera o segundo (hold-and-wait). Se o contexto for
// cancelado no meio, devolve o que já pegou e retorna false sem segurar nada.
func (m *Mesa) pegarEmOrdem(ctx context.Context, primeiro, segundo int) bool {
	select {
	case <-m.garfos[primeiro]:
	case <-ctx.Done():
		return false
	}
	if !dormir(ctx, m.pausa) {
		m.garfos[primeiro] <- struct{}{}
		return false
	}
	if m.esperandoSegundo.Add(1) == int32(m.n) {
		m.onceDeadlock.Do(func() { close(m.deadlock) })
	}
	select {
	case <-m.garfos[segundo]:
		m.esperandoSegundo.Add(-1)
		return true
	case <-ctx.Done():
		m.esperandoSegundo.Add(-1)
		m.garfos[primeiro] <- struct{}{}
		return false
	}
}

// devolver nunca bloqueia: o channel tem capacidade 1 e está vazio, porque o
// token está com quem devolve.
func (m *Mesa) devolver(a, b int) {
	m.garfos[a] <- struct{}{}
	m.garfos[b] <- struct{}{}
}

// Estrategia define como o filósofo i adquire e libera os dois garfos.
type Estrategia interface {
	Nome() string
	// Adquirir bloqueia até o filósofo i ter os dois garfos. Retorna false
	// se o contexto foi cancelado; nesse caso não segura nenhum recurso.
	Adquirir(ctx context.Context, i int) bool
	Liberar(i int)
	// Parar encerra goroutines auxiliares (garçom). Chamado depois que
	// todos os filósofos terminaram.
	Parar()
}

// ---------------------------------------------------------------------------
// Base: todos pegam o esquerdo e depois o direito. Sujeita a deadlock.

type Base struct{ m *Mesa }

func (b *Base) Nome() string { return "base" }
func (b *Base) Adquirir(ctx context.Context, i int) bool {
	return b.m.pegarEmOrdem(ctx, b.m.esquerdo(i), b.m.direito(i))
}
func (b *Base) Liberar(i int) { b.m.devolver(b.m.esquerdo(i), b.m.direito(i)) }
func (b *Base) Parar()        {}

// ---------------------------------------------------------------------------
// Hierarquia de recursos: cada filósofo pega primeiro o garfo de menor
// índice. Na prática só o filósofo N-1 inverte a ordem (pega o garfo 0
// antes do N-1), o que quebra a simetria e a espera circular.

type Hierarquia struct{ m *Mesa }

func (h *Hierarquia) Nome() string { return "hierarquia" }
func (h *Hierarquia) Adquirir(ctx context.Context, i int) bool {
	a, b := h.m.esquerdo(i), h.m.direito(i)
	if a > b {
		a, b = b, a
	}
	return h.m.pegarEmOrdem(ctx, a, b)
}
func (h *Hierarquia) Liberar(i int) { h.m.devolver(h.m.esquerdo(i), h.m.direito(i)) }
func (h *Hierarquia) Parar()        {}

// ---------------------------------------------------------------------------
// Limite de lugares: no máximo N-1 filósofos à mesa ao mesmo tempo. O
// "porteiro" é um semáforo de contagem implementado como channel com
// capacidade N-1 (enviar = ocupar lugar, receber = liberar). Dentro da mesa
// a ordem é a mesma da base (esquerdo, depois direito).

type Limite struct {
	m       *Mesa
	lugares chan struct{}
}

func novoLimite(m *Mesa) *Limite {
	return &Limite{m: m, lugares: make(chan struct{}, m.n-1)}
}

func (l *Limite) Nome() string { return "limite" }
func (l *Limite) Adquirir(ctx context.Context, i int) bool {
	select {
	case l.lugares <- struct{}{}:
	case <-ctx.Done():
		return false
	}
	if !l.m.pegarEmOrdem(ctx, l.m.esquerdo(i), l.m.direito(i)) {
		<-l.lugares
		return false
	}
	return true
}
func (l *Limite) Liberar(i int) {
	l.m.devolver(l.m.esquerdo(i), l.m.direito(i))
	<-l.lugares
}
func (l *Limite) Parar() {}

// ---------------------------------------------------------------------------
// Garçom: uma goroutine central é dona do estado dos garfos e concede os
// dois de uma vez. O filósofo só toca nos channels dos garfos depois da
// concessão, então nunca segura um garfo esperando o outro.
//
// Política de atendimento: fila de pedidos em ordem de chegada. O garçom
// percorre a fila; um pedido é atendido se os dois garfos estão livres e
// nenhum pedido mais antigo ainda não atendido está esperando por eles
// (reserva). A reserva impede que vizinhos "furem a fila" indefinidamente,
// que é o que causaria starvation numa política gulosa.

type pedido struct {
	id       int
	resposta chan struct{} // capacidade 1: o garçom nunca bloqueia ao responder
}

type Garcom struct {
	m          *Mesa
	reserva    bool // false = política gulosa (sem garantia contra starvation)
	pedidos    chan pedido
	devolucoes chan int
	fim        chan struct{}
	terminou   chan struct{}
}

func novoGarcom(m *Mesa, reserva bool) *Garcom {
	g := &Garcom{
		m:          m,
		reserva:    reserva,
		pedidos:    make(chan pedido),
		devolucoes: make(chan int),
		fim:        make(chan struct{}),
		terminou:   make(chan struct{}),
	}
	go g.atender()
	return g
}

func (g *Garcom) atender() {
	defer close(g.terminou)
	n := g.m.n
	livre := make([]bool, n)
	for i := range livre {
		livre[i] = true
	}
	var fila []pedido
	reservado := make([]bool, n)
	for {
		select {
		case p := <-g.pedidos:
			fila = append(fila, p)
		case id := <-g.devolucoes:
			livre[g.m.esquerdo(id)] = true
			livre[g.m.direito(id)] = true
		case <-g.fim:
			return
		}
		for i := range reservado {
			reservado[i] = false
		}
		restante := fila[:0]
		for _, p := range fila {
			e, d := g.m.esquerdo(p.id), g.m.direito(p.id)
			if livre[e] && livre[d] && !reservado[e] && !reservado[d] {
				livre[e], livre[d] = false, false
				p.resposta <- struct{}{}
			} else {
				if g.reserva {
					reservado[e], reservado[d] = true, true
				}
				restante = append(restante, p)
			}
		}
		fila = restante
	}
}

func (g *Garcom) Nome() string { return "garcom" }
func (g *Garcom) Adquirir(ctx context.Context, i int) bool {
	p := pedido{id: i, resposta: make(chan struct{}, 1)}
	select {
	case g.pedidos <- p:
	case <-ctx.Done():
		return false
	}
	select {
	case <-p.resposta:
	case <-ctx.Done():
		return false
	}
	// Concedido: os dois garfos estão livres, a ordem não importa e as
	// recepções não bloqueiam de fato.
	return g.m.pegarEmOrdem(ctx, g.m.esquerdo(i), g.m.direito(i))
}

// Liberar devolve os tokens aos channels ANTES de avisar o garçom, para que
// o próximo filósofo autorizado encontre os garfos já disponíveis.
func (g *Garcom) Liberar(i int) {
	g.m.devolver(g.m.esquerdo(i), g.m.direito(i))
	g.devolucoes <- i
}

func (g *Garcom) Parar() {
	close(g.fim)
	<-g.terminou
}

// GarcomReserva liga a reserva FIFO do garçom (padrão). Configurada pela
// flag -garcom-reserva; existe para medir o custo da garantia anti-starvation.
var GarcomReserva = true

func novaEstrategia(nome string, m *Mesa) Estrategia {
	switch nome {
	case "base":
		return &Base{m: m}
	case "hierarquia":
		return &Hierarquia{m: m}
	case "limite":
		return novoLimite(m)
	case "garcom":
		return novoGarcom(m, GarcomReserva)
	}
	return nil
}
