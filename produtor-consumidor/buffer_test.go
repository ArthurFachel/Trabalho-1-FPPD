package main

import (
	"runtime"
	"testing"
	"time"
)

// executarComPrazo falha o teste em vez de travar até o timeout de 10 min
// do go test. Um consumidor preso (permissão perdida, pílula faltando)
// aparece aqui como "travou".
func executarComPrazo(t *testing.T, v string, cfg Config) Resultado {
	t.Helper()
	ch := make(chan Resultado, 1)
	go func() { ch <- executar(v, cfg, false) }()
	select {
	case r := <-ch:
		return r
	case <-time.After(30 * time.Second):
		t.Fatalf("%s K=%d P=%d C=%d: travou (encerramento nao terminou em 30s)", v, cfg.K, cfg.P, cfg.C)
	}
	return Resultado{}
}

// Estresse sem trabalho simulado: máxima contenção nos semáforos e mutexes.
func TestSemContencao(t *testing.T) {
	for _, v := range []string{"channel", "semaforo"} {
		for _, k := range []int{1, 2, 7, 100} {
			for _, pc := range [][2]int{{1, 1}, {1, 8}, {8, 1}, {16, 16}} {
				cfg := Config{P: pc[0], C: pc[1], K: k, M: 2000, Timeout: time.Millisecond,
					Amostragem: time.Millisecond, Seed: 7}
				r := executarComPrazo(t, v, cfg)
				if !r.ok() {
					t.Fatalf("%s K=%d P=%d C=%d: prod=%d cons=%d perdidos=%d dup=%d",
						v, k, pc[0], pc[1], r.Produzidos, r.Consumidos, r.Perdidos, r.Duplicados)
				}
			}
		}
	}
}

// Timeout de 1 microssegundo: o consumidor 0 estoura o prazo o tempo todo
// enquanto itens continuam chegando. É o cenário em que um V() pode
// escolher o consumidor entre o time.After disparar e ele sair da fila.
// Se PTimeout tratasse isso errado, apareceria item perdido ou consumidor
// preso no fim.
func TestTimeoutAgressivo(t *testing.T) {
	for _, v := range []string{"channel", "semaforo"} {
		for _, k := range []int{1, 4} {
			cfg := Config{P: 4, C: 4, K: k, M: 3000, Timeout: time.Microsecond,
				Produzir: 20 * time.Microsecond, Amostragem: time.Millisecond, Seed: 3}
			r := executarComPrazo(t, v, cfg)
			if !r.ok() {
				t.Fatalf("%s K=%d: perdidos=%d dup=%d prod=%d cons=%d", v, k, r.Perdidos, r.Duplicados, r.Produzidos, r.Consumidos)
			}
			if r.Cons[0].Timeouts == 0 {
				t.Fatalf("%s K=%d: esperava timeouts no consumidor 0", v, k)
			}
		}
	}
}

// Consumidor só com timeout e produtores lentos: o comportamento
// alternativo tem que disparar, e mesmo assim nada se perde.
func TestComportamentoAlternativo(t *testing.T) {
	for _, v := range []string{"channel", "semaforo"} {
		cfg := Config{P: 1, C: 1, K: 5, M: 20, Produzir: 4 * time.Millisecond,
			Timeout: time.Millisecond, Amostragem: time.Millisecond, Seed: 1}
		r := executarComPrazo(t, v, cfg)
		if !r.ok() || r.Cons[0].Alternativo == 0 {
			t.Fatalf("%s: ok=%v alternativo=%d", v, r.ok(), r.Cons[0].Alternativo)
		}
	}
}

// Semáforo isolado: PTimeout concorrente com V. Toda permissão emitida por V
// tem que ser obtida por exatamente um P/PTimeout bem-sucedido.
func TestSemaforoConservaPermissoes(t *testing.T) {
	s := novoSemaforo(0)
	const total = 20000
	got := make(chan int, 64)
	stop := make(chan struct{})
	for w := 0; w < 8; w++ {
		go func() {
			n := 0
			for {
				select {
				case <-stop:
					got <- n
					return
				default:
				}
				if s.PTimeout(time.Microsecond) {
					n++
				}
			}
		}()
	}
	for i := 0; i < total; i++ {
		s.V()
		if i%100 == 0 {
			runtime.Gosched()
		}
	}
	// espera as permissões serem consumidas
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		c := s.cont
		s.mu.Unlock()
		if c == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(stop)
	soma := 0
	for w := 0; w < 8; w++ {
		soma += <-got
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if soma+s.cont != total {
		t.Fatalf("permissoes: obtidas=%d restantes=%d emitidas=%d", soma, s.cont, total)
	}
	if len(s.fila) != 0 {
		t.Fatalf("fila deveria estar vazia, tem %d", len(s.fila))
	}
}
