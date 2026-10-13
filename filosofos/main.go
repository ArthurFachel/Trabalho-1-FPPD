// Jantar dos Filósofos com channels como garfos.
//
// Uso:
//
//	go run ./filosofos -n 5 -r 100 -estrategia todas
//
// Veja README.md para todas as flags.
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	N, R    int
	Pool    bool // R*N refeições compartilhadas em vez de R por filósofo
	Pensar  time.Duration
	Comer   time.Duration
	Pausa   time.Duration
	Seed    uint64
	Timeout time.Duration
}

type Stats struct {
	Refeicoes   int
	EsperaTotal time.Duration
	EsperaMax   time.Duration
}

type Resultado struct {
	Estrategia string
	Cfg        Config
	Deadlock   bool
	Timeout    bool
	Duracao    time.Duration
	Por        []Stats
}

// filosofo executa o ciclo pensar -> pegar garfos -> comer -> devolver.
// Cada filósofo escreve só no próprio Stats; a leitura acontece depois do
// wg.Wait(), que estabelece a relação happens-before. Sem mutex e sem race.
func filosofo(ctx context.Context, i int, cfg Config, e Estrategia, orcamento *atomic.Int64, st *Stats) {
	rng := rand.New(rand.NewPCG(cfg.Seed, uint64(i)))
	for it := 0; ; it++ {
		if cfg.Pool {
			// Reserva uma refeição do orçamento global antes de tentar.
			if orcamento.Add(-1) < 0 {
				return
			}
		} else if it >= cfg.R {
			return
		}
		if cfg.Pensar > 0 && !dormir(ctx, time.Duration(rng.Int64N(int64(cfg.Pensar)+1))) {
			return
		}
		t0 := time.Now()
		if !e.Adquirir(ctx, i) {
			return
		}
		w := time.Since(t0)
		st.EsperaTotal += w
		if w > st.EsperaMax {
			st.EsperaMax = w
		}
		st.Refeicoes++
		if cfg.Comer > 0 {
			time.Sleep(cfg.Comer) // come sem ser interrompido; tempo finito
		}
		e.Liberar(i)
	}
}

func executar(nome string, cfg Config) Resultado {
	m := novaMesa(cfg.N, cfg.Pausa)
	e := novaEstrategia(nome, m)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	var orcamento atomic.Int64
	orcamento.Store(int64(cfg.N * cfg.R))

	por := make([]Stats, cfg.N)
	var wg sync.WaitGroup
	inicio := time.Now()
	for i := 0; i < cfg.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			filosofo(ctx, i, cfg, e, &orcamento, &por[i])
		}(i)
	}

	// Quando o detector dispara, cancela o contexto: os filósofos presos no
	// segundo garfo devolvem o primeiro e saem. Esse é o "desfazer" do
	// deadlock pra o programa poder terminar de forma ordenada.
	terminou := make(chan struct{})
	go func() { wg.Wait(); close(terminou) }()

	res := Resultado{Estrategia: nome, Cfg: cfg, Por: por}
	select {
	case <-terminou:
	case <-m.deadlock:
		res.Deadlock = true
		cancel()
		<-terminou
	case <-ctx.Done():
		res.Timeout = true
		<-terminou
	}
	res.Duracao = time.Since(inicio)
	e.Parar()

	// Sanidade: todo garfo deve ter voltado pra mesa.
	for i, g := range m.garfos {
		if len(g) != 1 {
			fmt.Fprintf(os.Stderr, "ERRO: garfo %d não foi devolvido\n", i)
			os.Exit(2)
		}
	}
	return res
}

// jain calcula o índice de fairness de Jain: (sum x)^2 / (n * sum x^2).
// Vale 1 quando todos recebem igual e 1/n quando um só recebe tudo.
func jain(xs []float64) float64 {
	var s, s2 float64
	for _, x := range xs {
		s += x
		s2 += x * x
	}
	if s2 == 0 {
		return 1
	}
	return s * s / (float64(len(xs)) * s2)
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func imprimir(r Resultado) {
	status := "ok"
	if r.Deadlock {
		status = "DEADLOCK detectado (todos com 1 garfo esperando o 2o)"
	} else if r.Timeout {
		status = "TIMEOUT"
	}
	total := 0
	for _, s := range r.Por {
		total += s.Refeicoes
	}
	fmt.Printf("\n== %s | N=%d R=%d pool=%v | %s | %d refeicoes em %v\n",
		r.Estrategia, r.Cfg.N, r.Cfg.R, r.Cfg.Pool, status, total, r.Duracao.Round(time.Millisecond))
	fmt.Printf("%4s %10s %14s %14s\n", "fil", "refeicoes", "espera_med_ms", "espera_max_ms")
	refs := make([]float64, len(r.Por))
	meds := make([]float64, len(r.Por))
	for i, s := range r.Por {
		med := 0.0
		if s.Refeicoes > 0 {
			med = ms(s.EsperaTotal) / float64(s.Refeicoes)
		}
		refs[i], meds[i] = float64(s.Refeicoes), med
		fmt.Printf("%4d %10d %14.3f %14.3f\n", i, s.Refeicoes, med, ms(s.EsperaMax))
	}
	fmt.Printf("Jain(refeicoes)=%.4f  Jain(espera_media)=%.4f  espera_media min/max=%.3f/%.3f ms\n",
		jain(refs), jain(meds), minf(meds), maxf(meds))
}

func minf(xs []float64) float64 {
	m := math.Inf(1)
	for _, x := range xs {
		m = math.Min(m, x)
	}
	return m
}

func maxf(xs []float64) float64 {
	m := math.Inf(-1)
	for _, x := range xs {
		m = math.Max(m, x)
	}
	return m
}

func escreverCSV(path string, rs []Resultado, rep int) error {
	novo := false
	if _, err := os.Stat(path); os.IsNotExist(err) {
		novo = true
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if novo {
		w.Write([]string{"estrategia", "n", "r", "pool", "pensar_ms", "comer_ms", "pausa_ms", "seed", "rep",
			"deadlock", "duracao_ms", "filosofo", "refeicoes", "espera_media_ms", "espera_max_ms"})
	}
	f3 := func(x float64) string { return strconv.FormatFloat(x, 'f', 4, 64) }
	for _, r := range rs {
		for i, s := range r.Por {
			med := 0.0
			if s.Refeicoes > 0 {
				med = ms(s.EsperaTotal) / float64(s.Refeicoes)
			}
			w.Write([]string{r.Estrategia, strconv.Itoa(r.Cfg.N), strconv.Itoa(r.Cfg.R), strconv.FormatBool(r.Cfg.Pool),
				f3(ms(r.Cfg.Pensar)), f3(ms(r.Cfg.Comer)), f3(ms(r.Cfg.Pausa)), strconv.FormatUint(r.Cfg.Seed, 10),
				strconv.Itoa(rep), strconv.FormatBool(r.Deadlock), f3(ms(r.Duracao)), strconv.Itoa(i),
				strconv.Itoa(s.Refeicoes), f3(med), f3(ms(s.EsperaMax))})
		}
	}
	w.Flush()
	return w.Error()
}

func main() {
	var cfg Config
	var estr, csvPath string
	var reps int
	flag.IntVar(&cfg.N, "n", 5, "número de filósofos (>= 2)")
	flag.IntVar(&cfg.R, "r", 100, "iterações (refeições) por filósofo; com -pool, o total é N*R")
	flag.BoolVar(&cfg.Pool, "pool", false, "orçamento global de N*R refeições disputado entre os filósofos")
	flag.DurationVar(&cfg.Pensar, "pensar", 2*time.Millisecond, "tempo máximo de pensar (uniforme em [0, pensar])")
	flag.DurationVar(&cfg.Comer, "comer", 1*time.Millisecond, "tempo de comer")
	flag.DurationVar(&cfg.Pausa, "pausa", 1*time.Millisecond, "pausa entre pegar o 1o e o 2o garfo (aumenta a chance de deadlock na base)")
	flag.Uint64Var(&cfg.Seed, "seed", 1, "semente dos geradores aleatórios")
	flag.DurationVar(&cfg.Timeout, "timeout", 60*time.Second, "limite de segurança por execução")
	flag.StringVar(&estr, "estrategia", "todas", "base|hierarquia|limite|garcom|todas (lista separada por vírgula também vale)")
	flag.IntVar(&reps, "reps", 1, "repetições (seed incrementada a cada uma)")
	flag.StringVar(&csvPath, "csv", "", "arquivo CSV para acrescentar os dados por filósofo")
	flag.BoolVar(&GarcomReserva, "garcom-reserva", true, "garçom com reserva FIFO (false = guloso, sem garantia contra starvation)")
	quieto := flag.Bool("q", false, "não imprime a tabela por filósofo (só o resumo)")
	flag.Parse()

	if cfg.N < 2 || cfg.R < 1 {
		fmt.Fprintln(os.Stderr, "precisa de n >= 2 e r >= 1")
		os.Exit(1)
	}
	nomes := []string{"base", "hierarquia", "limite", "garcom"}
	if estr != "todas" {
		nomes = strings.Split(estr, ",")
	}
	for _, n := range nomes {
		e := novaEstrategia(n, novaMesa(2, 0))
		if e == nil {
			fmt.Fprintf(os.Stderr, "estrategia desconhecida: %s\n", n)
			os.Exit(1)
		}
		e.Parar() // o garçom sobe uma goroutine; para antes de medir a linha de base
	}

	gorAntes := runtime.NumGoroutine()
	deadlocks := map[string]int{}
	baseSeed := cfg.Seed
	for rep := 0; rep < reps; rep++ {
		cfg.Seed = baseSeed + uint64(rep)
		var rs []Resultado
		for _, n := range nomes {
			r := executar(n, cfg)
			if r.Deadlock {
				deadlocks[n]++
			}
			if !*quieto {
				imprimir(r)
			}
			rs = append(rs, r)
		}
		if csvPath != "" {
			if err := escreverCSV(csvPath, rs, rep); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
	}
	fmt.Printf("\nResumo (%d repeticoes):", reps)
	for _, n := range nomes {
		fmt.Printf(" %s=%d deadlock(s)", n, deadlocks[n])
	}
	fmt.Println()

	// Encerramento ordenado: nenhuma goroutine criada pelas execuções deve
	// sobrar. Dá um respiro curto pro runtime recolher goroutines que já
	// retornaram (a que fecha `terminou`, por exemplo).
	sobra := 0
	for t := 0; t < 50; t++ {
		sobra = runtime.NumGoroutine() - gorAntes
		if sobra <= 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if sobra > 0 {
		fmt.Fprintf(os.Stderr, "ERRO: %d goroutine(s) ainda vivas ao final\n", sobra)
		os.Exit(3)
	}
	fmt.Printf("Encerramento ordenado: goroutines antes=%d depois=%d\n", gorAntes, runtime.NumGoroutine())
}
