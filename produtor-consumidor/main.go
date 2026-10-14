// Produtor/Consumidor com buffer de tamanho fixo: uma versão com channel,
// outra com semáforos.
//
// Uso:
//
//	go run ./produtor-consumidor -p 4 -c 4 -k 10 -m 500 -versao todas
//
// Veja README.md para todas as flags.
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Config struct {
	P, C, K, M int
	Produzir   time.Duration // tempo médio (exponencial) pra produzir um item
	Consumir   time.Duration // tempo médio (exponencial) pra consumir um item
	Timeout    time.Duration // prazo do consumidor 0 (select + time.After)
	Amostragem time.Duration // de quanto em quanto tempo mede a ocupação
	Seed       uint64
}

type StatsConsumidor struct {
	Itens       int
	Espera      time.Duration // tempo parado esperando item
	Timeouts    int           // só o consumidor 0
	Alternativo int           // quantas vezes rodou o comportamento alternativo
	vistos      []Item
}

type StatsProdutor struct {
	Itens    int
	Bloqueio time.Duration // tempo parado no Put (buffer cheio)
}

type Resultado struct {
	Versao     string
	Cfg        Config
	Duracao    time.Duration
	Prod       []StatsProdutor
	Cons       []StatsConsumidor
	OcupMedia  float64
	Amostras   int
	Produzidos int
	Consumidos int
	Duplicados int
	Perdidos   int
}

func dormirExp(rng *rand.Rand, media time.Duration) {
	if media > 0 {
		time.Sleep(time.Duration(rng.ExpFloat64() * float64(media)))
	}
}

func produtor(id int, cfg Config, b Buffer, st *StatsProdutor) {
	rng := rand.New(rand.NewPCG(cfg.Seed, uint64(1000+id)))
	for s := 0; s < cfg.M; s++ {
		dormirExp(rng, cfg.Produzir)
		t0 := time.Now()
		b.Put(Item{Produtor: id, Seq: s})
		st.Bloqueio += time.Since(t0)
		st.Itens++
	}
}

// consumidor normal: para no Get até chegar item ou o buffer acabar.
func consumidor(id int, cfg Config, b Buffer, st *StatsConsumidor) {
	rng := rand.New(rand.NewPCG(cfg.Seed, uint64(2000+id)))
	for {
		t0 := time.Now()
		it, ok := b.Get()
		st.Espera += time.Since(t0)
		if !ok {
			return
		}
		st.Itens++
		st.vistos = append(st.vistos, it)
		dormirExp(rng, cfg.Consumir)
	}
}

// consumidorComTimeout: se nada chega em cfg.Timeout, faz outra coisa e
// tenta de novo. Nunca desiste do buffer por causa do prazo; só sai quando o
// buffer diz que acabou, pra não deixar item sem consumidor.
func consumidorComTimeout(id int, cfg Config, b Buffer, st *StatsConsumidor, verboso bool) {
	rng := rand.New(rand.NewPCG(cfg.Seed, uint64(2000+id)))
	for {
		t0 := time.Now()
		it, ok, timeout := b.GetTimeout(cfg.Timeout)
		st.Espera += time.Since(t0)
		if timeout {
			st.Timeouts++
			comportamentoAlternativo(st)
			if verboso {
				fmt.Printf("  [consumidor %d] nada em %v, executou tarefa alternativa (%d)\n", id, cfg.Timeout, st.Timeouts)
			}
			continue
		}
		if !ok {
			return
		}
		st.Itens++
		st.vistos = append(st.vistos, it)
		dormirExp(rng, cfg.Consumir)
	}
}

func comportamentoAlternativo(st *StatsConsumidor) {
	st.Alternativo++
	st.vistos = st.vistos[:len(st.vistos):len(st.vistos)] // libera a capacidade sobrando
}

func novoBuffer(versao string, k int) Buffer {
	switch versao {
	case "channel":
		return novoBufferChannel(k)
	case "semaforo":
		return novoBufferSemaforo(k)
	}
	return nil
}

func executar(versao string, cfg Config, verboso bool) Resultado {
	b := novoBuffer(versao, cfg.K)
	res := Resultado{Versao: versao, Cfg: cfg,
		Prod: make([]StatsProdutor, cfg.P), Cons: make([]StatsConsumidor, cfg.C)}

	// Amostrador: lê o tamanho do buffer em intervalos iguais, então a média
	// das medidas é a ocupação média.
	pararAmostra := make(chan struct{})
	amostraFeita := make(chan struct{})
	var somaOcup, nAmostras int
	go func() {
		defer close(amostraFeita)
		tk := time.NewTicker(cfg.Amostragem)
		defer tk.Stop()
		for {
			select {
			case <-tk.C:
				somaOcup += b.Ocupacao()
				nAmostras++
			case <-pararAmostra:
				return
			}
		}
	}()

	var wgProd, wgCons sync.WaitGroup
	inicio := time.Now()
	for i := 0; i < cfg.C; i++ {
		wgCons.Add(1)
		go func(i int) {
			defer wgCons.Done()
			if i == 0 {
				consumidorComTimeout(i, cfg, b, &res.Cons[i], verboso)
			} else {
				consumidor(i, cfg, b, &res.Cons[i])
			}
		}(i)
	}
	for i := 0; i < cfg.P; i++ {
		wgProd.Add(1)
		go func(i int) {
			defer wgProd.Done()
			produtor(i, cfg, b, &res.Prod[i])
		}(i)
	}

	// Encerramento: espera os produtores, avisa o fim no buffer (close ou
	// pílulas) e espera os consumidores esvaziarem tudo e saírem.
	wgProd.Wait()
	b.Fechar(cfg.C)
	wgCons.Wait()
	res.Duracao = time.Since(inicio)
	close(pararAmostra)
	<-amostraFeita

	if nAmostras > 0 {
		res.OcupMedia = float64(somaOcup) / float64(nAmostras)
	}
	res.Amostras = nAmostras
	verificar(&res)
	return res
}

// verificar confere que cada item foi consumido uma vez só. Olhar o total
// não basta: um perdido e um duplicado se anulam.
func verificar(r *Resultado) {
	vistos := make([][]int, r.Cfg.P)
	for p := range vistos {
		vistos[p] = make([]int, r.Cfg.M)
	}
	for _, s := range r.Prod {
		r.Produzidos += s.Itens
	}
	for _, c := range r.Cons {
		r.Consumidos += c.Itens
		for _, it := range c.vistos {
			vistos[it.Produtor][it.Seq]++
		}
	}
	for p := range vistos {
		for _, n := range vistos[p] {
			switch {
			case n == 0:
				r.Perdidos++
			case n > 1:
				r.Duplicados += n - 1
			}
		}
	}
}

func (r Resultado) ok() bool {
	return r.Produzidos == r.Cfg.P*r.Cfg.M && r.Produzidos == r.Consumidos && r.Perdidos == 0 && r.Duplicados == 0
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func imprimir(r Resultado) {
	thr := float64(r.Consumidos) / r.Duracao.Seconds()
	fmt.Printf("\n== %s | P=%d C=%d K=%d M=%d | %v | throughput=%.0f itens/s | ocupacao media=%.2f/%d (%d amostras)\n",
		r.Versao, r.Cfg.P, r.Cfg.C, r.Cfg.K, r.Cfg.M, r.Duracao.Round(time.Millisecond), thr, r.OcupMedia, r.Cfg.K, r.Amostras)
	for i, c := range r.Cons {
		extra := ""
		if i == 0 {
			extra = fmt.Sprintf("  timeouts=%d (comportamento alternativo %d vezes)", c.Timeouts, c.Alternativo)
		}
		fmt.Printf("  consumidor %d: %6d itens, espera %8.1f ms%s\n", i, c.Itens, ms(c.Espera), extra)
	}
	for i, p := range r.Prod {
		fmt.Printf("  produtor   %d: %6d itens, bloqueado %8.1f ms\n", i, p.Itens, ms(p.Bloqueio))
	}
	st := "OK"
	if !r.ok() {
		st = "FALHOU"
	}
	fmt.Printf("  verificacao: produzidos=%d consumidos=%d perdidos=%d duplicados=%d -> %s\n",
		r.Produzidos, r.Consumidos, r.Perdidos, r.Duplicados, st)
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
		w.Write([]string{"versao", "p", "c", "k", "m", "produzir_ms", "consumir_ms", "timeout_ms", "seed", "rep",
			"duracao_ms", "throughput", "ocup_media", "amostras", "papel", "id", "itens", "espera_ms", "timeouts", "ok"})
	}
	f4 := func(x float64) string { return strconv.FormatFloat(x, 'f', 4, 64) }
	for _, r := range rs {
		base := []string{r.Versao, strconv.Itoa(r.Cfg.P), strconv.Itoa(r.Cfg.C), strconv.Itoa(r.Cfg.K), strconv.Itoa(r.Cfg.M),
			f4(ms(r.Cfg.Produzir)), f4(ms(r.Cfg.Consumir)), f4(ms(r.Cfg.Timeout)), strconv.FormatUint(r.Cfg.Seed, 10),
			strconv.Itoa(rep), f4(ms(r.Duracao)), f4(float64(r.Consumidos) / r.Duracao.Seconds()), f4(r.OcupMedia),
			strconv.Itoa(r.Amostras)}
		for i, c := range r.Cons {
			w.Write(append(append([]string{}, base...), "consumidor", strconv.Itoa(i), strconv.Itoa(c.Itens),
				f4(ms(c.Espera)), strconv.Itoa(c.Timeouts), strconv.FormatBool(r.ok())))
		}
		for i, p := range r.Prod {
			w.Write(append(append([]string{}, base...), "produtor", strconv.Itoa(i), strconv.Itoa(p.Itens),
				f4(ms(p.Bloqueio)), "0", strconv.FormatBool(r.ok())))
		}
	}
	w.Flush()
	return w.Error()
}

func parseKs(s string) ([]int, error) {
	var ks []int
	for _, x := range strings.Split(s, ",") {
		k, err := strconv.Atoi(strings.TrimSpace(x))
		if err != nil || k < 1 {
			return nil, fmt.Errorf("K invalido: %q (precisa ser >= 1)", x)
		}
		ks = append(ks, k)
	}
	return ks, nil
}

func main() {
	var cfg Config
	var versao, ksStr, csvPath string
	var reps int
	flag.IntVar(&cfg.P, "p", 4, "número de produtores")
	flag.IntVar(&cfg.C, "c", 4, "número de consumidores (o consumidor 0 usa timeout)")
	flag.StringVar(&ksStr, "k", "1,10,100", "capacidade(s) do buffer, separadas por vírgula")
	flag.IntVar(&cfg.M, "m", 500, "itens produzidos por produtor")
	flag.DurationVar(&cfg.Produzir, "produzir", 1*time.Millisecond, "tempo médio para produzir um item (exponencial; 0 = sem trabalho)")
	flag.DurationVar(&cfg.Consumir, "consumir", 1*time.Millisecond, "tempo médio para consumir um item (exponencial; 0 = sem trabalho)")
	flag.DurationVar(&cfg.Timeout, "timeout", 3*time.Millisecond, "prazo do consumidor 0 antes do comportamento alternativo")
	flag.DurationVar(&cfg.Amostragem, "amostragem", 200*time.Microsecond, "período de amostragem da ocupação do buffer")
	flag.Uint64Var(&cfg.Seed, "seed", 1, "semente")
	flag.StringVar(&versao, "versao", "todas", "channel|semaforo|todas")
	flag.IntVar(&reps, "reps", 1, "repetições (seed incrementada a cada uma)")
	flag.StringVar(&csvPath, "csv", "", "arquivo CSV para acrescentar os dados")
	verboso := flag.Bool("v", false, "imprime cada timeout do consumidor 0")
	flag.Parse()

	ks, err := parseKs(ksStr)
	if err != nil || cfg.P < 1 || cfg.C < 1 || cfg.M < 1 {
		fmt.Fprintln(os.Stderr, "parametros invalidos:", err)
		os.Exit(1)
	}
	versoes := []string{"channel", "semaforo"}
	if versao != "todas" {
		versoes = strings.Split(versao, ",")
	}
	for _, v := range versoes {
		if novoBuffer(v, 1) == nil {
			fmt.Fprintf(os.Stderr, "versao desconhecida: %s\n", v)
			os.Exit(1)
		}
	}

	gorAntes := runtime.NumGoroutine()
	falhas := 0
	baseSeed := cfg.Seed
	for rep := 0; rep < reps; rep++ {
		cfg.Seed = baseSeed + uint64(rep)
		var rs []Resultado
		for _, k := range ks {
			for _, v := range versoes {
				c := cfg
				c.K = k
				r := executar(v, c, *verboso)
				if !r.ok() {
					falhas++
				}
				imprimir(r)
				rs = append(rs, r)
			}
		}
		if csvPath != "" {
			if err := escreverCSV(csvPath, rs, rep); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
	}

	sobra := 0
	for t := 0; t < 50; t++ {
		if sobra = runtime.NumGoroutine() - gorAntes; sobra <= 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	fmt.Printf("\nEncerramento ordenado: goroutines antes=%d depois=%d | execucoes com falha de verificacao: %d\n",
		gorAntes, runtime.NumGoroutine(), falhas)
	if sobra > 0 || falhas > 0 {
		os.Exit(3)
	}
}
