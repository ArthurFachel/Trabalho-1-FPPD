package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Garfo é um channel de capacidade 1: token dentro = garfo livre. Como só
// existe um token, dois filósofos nunca pegam o mesmo garfo.
type Garfo chan struct{}

func novoGarfo() Garfo {
	g := make(Garfo, 1)
	g <- struct{}{}
	return g
}

// Mesa guarda os garfos e o detector de deadlock. esperandoSegundo conta
// quem está com um garfo esperando o outro: se chegar a N, cada um tem um
// garfo e ninguém vai soltar. É deadlock de verdade, não suspeita.
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

// dormir espera d; retorna false se o contexto foi cancelado antes.
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

// pegarEmOrdem segura o `primeiro` enquanto espera o `segundo`. Se o
// contexto cancelar no meio, devolve o que pegou e retorna false.
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

// devolver nunca trava: o token está na mão de quem devolve.
func (m *Mesa) devolver(a, b int) {
	m.garfos[a] <- struct{}{}
	m.garfos[b] <- struct{}{}
}

// Estrategia define como o filósofo i adquire e libera os dois garfos.
type Estrategia interface {
	Nome() string
	// Adquirir espera até ter os dois garfos. false = contexto cancelado,
	// e aí não fica com nenhum.
	Adquirir(ctx context.Context, i int) bool
	Liberar(i int)
	// Parar fecha as goroutines auxiliares (o garçom).
	Parar()
}

// ---------------------------------------------------------------------------
// Base: esquerda e depois direita. É a versão que trava.

type Base struct{ m *Mesa }

func (b *Base) Nome() string { return "base" }
func (b *Base) Adquirir(ctx context.Context, i int) bool {
	return b.m.pegarEmOrdem(ctx, b.m.esquerdo(i), b.m.direito(i))
}
func (b *Base) Liberar(i int) { b.m.devolver(b.m.esquerdo(i), b.m.direito(i)) }
func (b *Base) Parar()        {}

// ---------------------------------------------------------------------------
// Hierarquia: cada um pega primeiro o garfo de número menor. Só o último
// inverte a ordem, e isso já quebra a espera circular.

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
// Limite: no máximo N-1 na mesa, controlado por um channel com N-1 vagas.
// Dentro da mesa a ordem dos garfos é a mesma da base.

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
// Garçom: uma goroutine central libera os dois garfos de uma vez, então
// ninguém fica com um na mão esperando o outro. Atende por ordem de chegada
// e um pedido só passa se nenhum pedido mais antigo está esperando por
// aqueles garfos (a reserva). Sem reserva, o vizinho fura a fila.

type pedido struct {
	id       int
	resposta chan struct{} // capacidade 1: o garçom nunca trava ao responder
}

type Garcom struct {
	m          *Mesa
	reserva    bool // false = guloso, sem garantia de que todos comem
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
	// Autorizado: os dois estão livres, pegar aqui não trava.
	return g.m.pegarEmOrdem(ctx, g.m.esquerdo(i), g.m.direito(i))
}

// Liberar devolve os tokens antes de avisar o garçom, pro próximo
// autorizado já achar os garfos prontos.
func (g *Garcom) Liberar(i int) {
	g.m.devolver(g.m.esquerdo(i), g.m.direito(i))
	g.devolucoes <- i
}

func (g *Garcom) Parar() {
	close(g.fim)
	<-g.terminou
}

// GarcomReserva liga a reserva por ordem de chegada (padrão); a flag
// -garcom-reserva desliga, pra medir o custo dela.
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
