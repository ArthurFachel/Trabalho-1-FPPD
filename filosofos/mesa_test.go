package main

import (
	"testing"
	"time"
)

// Sem tempo de pensar e com pausa entre os garfos, a base trava sempre: todos
// pegam o esquerdo durante a pausa. O detector tem que perceber e o programa
// tem que terminar com todos os garfos devolvidos (executar confere isso).
func TestBaseTrava(t *testing.T) {
	for _, n := range []int{2, 5, 10} {
		cfg := Config{N: n, R: 50, Pausa: 2 * time.Millisecond, Seed: 1, Timeout: 10 * time.Second}
		r := executar("base", cfg)
		if !r.Deadlock {
			t.Fatalf("N=%d: base deveria travar neste cenário", n)
		}
	}
}

// Mesmo cenário adversarial: as três estratégias não podem travar e cada
// filósofo tem que completar as R refeições.
func TestEstrategiasNaoTravam(t *testing.T) {
	for _, e := range []string{"hierarquia", "limite", "garcom"} {
		for _, n := range []int{2, 3, 5, 10} {
			for seed := uint64(1); seed <= 5; seed++ {
				cfg := Config{N: n, R: 30, Pausa: time.Millisecond, Seed: seed, Timeout: 20 * time.Second}
				r := executar(e, cfg)
				if r.Deadlock || r.Timeout {
					t.Fatalf("%s N=%d seed=%d: deadlock=%v timeout=%v", e, n, seed, r.Deadlock, r.Timeout)
				}
				for i, s := range r.Por {
					if s.Refeicoes != cfg.R {
						t.Fatalf("%s N=%d: filosofo %d comeu %d de %d", e, n, i, s.Refeicoes, cfg.R)
					}
				}
			}
		}
	}
}

// Sem nenhum tempo de trabalho: contenção máxima. Garante exclusão mútua
// contando quantos vizinhos comem ao mesmo tempo seria invasivo; aqui o
// teste é que tudo termina, e o -race cobre o acesso aos garfos.
func TestPoolSemTrabalho(t *testing.T) {
	for _, e := range []string{"hierarquia", "limite", "garcom"} {
		cfg := Config{N: 5, R: 500, Pool: true, Seed: 1, Timeout: 20 * time.Second}
		r := executar(e, cfg)
		total := 0
		for _, s := range r.Por {
			total += s.Refeicoes
		}
		if r.Deadlock || r.Timeout || total != cfg.N*cfg.R {
			t.Fatalf("%s: deadlock=%v timeout=%v total=%d", e, r.Deadlock, r.Timeout, total)
		}
	}
}
