#!/usr/bin/env python3
"""Resume os CSVs de resultados/ em tabelas (média ± desvio entre repetições).
Só stdlib. Escreve resultados/resumo.txt e relatorio/tabelas_bruto.tex.
"""
import csv
import statistics as st
from collections import defaultdict
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
RES = RAIZ / "resultados"


def jain(xs):
    s, s2 = sum(xs), sum(x * x for x in xs)
    return 1.0 if s2 == 0 else s * s / (len(xs) * s2)


def ms(xs):
    m = st.mean(xs)
    d = st.stdev(xs) if len(xs) > 1 else 0.0
    return m, d


def fmt(m, d, casas=2):
    return f"{m:.{casas}f} ± {d:.{casas}f}"


def filosofos(nome):
    rows = list(csv.DictReader(open(RES / nome)))
    # agrupa por (estrategia, rep)
    runs = defaultdict(list)
    for r in rows:
        runs[(r["estrategia"], int(r["rep"]))].append(r)
    por_estr = defaultdict(list)
    for (e, rep), rs in runs.items():
        rs.sort(key=lambda r: int(r["filosofo"]))
        refs = [int(r["refeicoes"]) for r in rs]
        esp = [float(r["espera_media_ms"]) for r in rs]
        espmax = [float(r["espera_max_ms"]) for r in rs]
        por_estr[e].append(dict(
            deadlock=rs[0]["deadlock"] == "true",
            dur=float(rs[0]["duracao_ms"]),
            refs=refs, esp=esp,
            jain_r=jain(refs), jain_e=jain(esp),
            minr=min(refs), maxr=max(refs),
            espmed=st.mean(esp), espmax=max(espmax),
            razao_esp=max(esp) / min(esp) if min(esp) > 0 else float("nan"),
        ))
    return por_estr


def produtor_consumidor(nome):
    rows = list(csv.DictReader(open(RES / nome)))
    runs = defaultdict(list)
    for r in rows:
        runs[(r["versao"], int(r["k"]), int(r["rep"]))].append(r)
    agg = defaultdict(list)
    for (v, k, rep), rs in runs.items():
        cons = sorted((r for r in rs if r["papel"] == "consumidor"), key=lambda r: int(r["id"]))
        prod = [r for r in rs if r["papel"] == "produtor"]
        agg[(v, k)].append(dict(
            thr=float(rs[0]["throughput"]),
            ocup=float(rs[0]["ocup_media"]),
            itens=[int(r["itens"]) for r in cons],
            espera_cons=st.mean(float(r["espera_ms"]) for r in cons),
            bloq_prod=st.mean(float(r["espera_ms"]) for r in prod),
            timeouts=int(cons[0]["timeouts"]),
            ok=all(r["ok"] == "true" for r in rs),
        ))
    return agg


def main():
    out = []
    tex = []

    cenarios_f = [
        ("fil_padrao.csv", "R=200 por filósofo, pensar U[0,2ms], comer 1ms, pausa 1ms"),
        ("fil_pool_comer1ms.csv", "pool N*R=1000, pensar 0, comer 1ms, pausa 0"),
        ("fil_pool_sem_trabalho.csv", "pool N*R=10000, pensar 0, comer 0, pausa 0"),
    ]
    for arq, desc in cenarios_f:
        d = filosofos(arq)
        guloso = RES / arq.replace(".csv", "_garcom_guloso.csv")
        if guloso.exists():
            d["garcom-guloso"] = filosofos(guloso.name)["garcom"]
        out.append(f"\n## Filósofos: {arq} ({desc}), N=5, 10 reps")
        out.append(f"{'estrategia':<11} {'deadl':>5} {'Jain(ref)':>16} {'ref min/max':>14} "
                   f"{'espera_med ms':>16} {'razao esp max/min':>18} {'espera_max ms':>14} {'duracao ms':>18}")
        tex.append(f"% {arq}")
        for e in ["base", "hierarquia", "limite", "garcom", "garcom-guloso"]:
            if e not in d:
                continue
            xs = d[e]
            ok = [x for x in xs if not x["deadlock"]]
            nd = len(xs) - len(ok)
            if not ok:
                out.append(f"{e:<11} {nd:>2}/{len(xs):<2} (todas travaram)")
                continue
            jr, jrd = ms([x["jain_r"] for x in ok])
            em, emd = ms([x["espmed"] for x in ok])
            rz, rzd = ms([x["razao_esp"] for x in ok])
            mn = min(x["minr"] for x in ok)
            mx = max(x["maxr"] for x in ok)
            emax = max(x["espmax"] for x in ok)
            du, dud = ms([x["dur"] for x in ok])
            out.append(f"{e:<11} {nd:>2}/{len(xs):<2} {fmt(jr, jrd, 4):>16} {mn:>6}/{mx:<7} "
                       f"{fmt(em, emd, 3):>16} {fmt(rz, rzd):>18} {emax:>14.2f} {fmt(du, dud, 0):>18}")
            tex.append(f"{e} & {nd}/{len(xs)} & {jr:.4f} $\\pm$ {jrd:.4f} & {mn}--{mx} & "
                       f"{em:.3f} $\\pm$ {emd:.3f} & {rz:.2f} & {emax:.2f} & {du:.0f} $\\pm$ {dud:.0f} \\\\")

    cenarios_p = [
        ("pc_equilibrado.csv", "produzir ~Exp(1ms), consumir ~Exp(1ms)"),
        ("pc_consumo_lento.csv", "produzir ~Exp(1ms), consumir ~Exp(1.5ms)"),
        ("pc_sem_trabalho.csv", "sem trabalho simulado, M=20000"),
    ]
    for arq, desc in cenarios_p:
        d = produtor_consumidor(arq)
        out.append(f"\n## Prod/Cons: {arq} ({desc}), P=4 C=4, 5 reps")
        out.append(f"{'versao':<9} {'K':>4} {'throughput it/s':>20} {'ocup media':>14} "
                   f"{'bloq prod ms':>16} {'espera cons ms':>16} {'itens/cons (media rep)':>28} {'timeouts c0':>11} ok")
        tex.append(f"% {arq}")
        for k in [1, 10, 100]:
            for v in ["channel", "semaforo"]:
                xs = d[(v, k)]
                th, thd = ms([x["thr"] for x in xs])
                oc, ocd = ms([x["ocup"] for x in xs])
                bp, bpd = ms([x["bloq_prod"] for x in xs])
                ec, ecd = ms([x["espera_cons"] for x in xs])
                itens = [round(st.mean(x["itens"][i] for x in xs)) for i in range(len(xs[0]["itens"]))]
                to = st.mean(x["timeouts"] for x in xs)
                ok = all(x["ok"] for x in xs)
                out.append(f"{v:<9} {k:>4} {fmt(th, thd, 0):>20} {fmt(oc, ocd):>14} {fmt(bp, bpd, 1):>16} "
                           f"{fmt(ec, ecd, 1):>16} {str(itens):>28} {to:>11.1f} {ok}")
                tex.append(f"{v} & {k} & {th:.0f} $\\pm$ {thd:.0f} & {oc:.2f} & {bp:.1f} & {ec:.1f} & "
                           f"{'/'.join(map(str, itens))} & {to:.1f} \\\\")

    texto = "\n".join(out).strip() + "\n"
    (RES / "resumo.txt").write_text(texto)
    (RAIZ / "relatorio").mkdir(exist_ok=True)
    (RAIZ / "relatorio" / "tabelas_bruto.tex").write_text("\n".join(tex) + "\n")
    print(texto)


if __name__ == "__main__":
    main()
