import atexit
import os
import shutil
import subprocess

import numpy as np
import pandas as pd
from flask import Flask, jsonify, request, send_from_directory

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
D = os.path.join(BASE_DIR, "data", "processed")
app=Flask(__name__,static_folder="static")
PIPELINE_PROCESS = None


def stop_pipeline():
    if PIPELINE_PROCESS is None or PIPELINE_PROCESS.poll() is not None:
        return
    PIPELINE_PROCESS.stdin.write("AEROPULSE_STOP\n")
    PIPELINE_PROCESS.stdin.flush()
    try:
        PIPELINE_PROCESS.wait(timeout=30)
    except subprocess.TimeoutExpired:
        PIPELINE_PROCESS.terminate()
        PIPELINE_PROCESS.wait(timeout=10)


def run_pipeline():
    global PIPELINE_PROCESS
    script = os.path.join(BASE_DIR, "pipeline", "run_pipeline.sh")
    if os.name == "nt":
        wsl = shutil.which("wsl.exe") or shutil.which("wsl")
        if not wsl:
            raise RuntimeError("WSL is required to run the Hadoop/Spark data pipeline.")
        project_path = subprocess.run(
            [wsl, "--exec", "wslpath", "-a", BASE_DIR],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        command = [
            wsl,
            "--exec",
            "bash",
            f"{project_path}/pipeline/run_pipeline.sh",
            project_path,
            "--keep-alive",
        ]
    else:
        command = ["bash", script, BASE_DIR, "--keep-alive"]
    process = subprocess.Popen(
        command,
        cwd=BASE_DIR,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )
    PIPELINE_PROCESS = process
    ready = False
    for line in process.stdout:
        print(line, end="", flush=True)
        if line.strip() == "AEROPULSE_PIPELINE_READY":
            ready = True
            break
    if not ready:
        return_code = process.wait()
        PIPELINE_PROCESS = None
        raise subprocess.CalledProcessError(return_code, command)
    atexit.register(stop_pipeline)


def load_data():
    global C, PR, CY, OT, DY, IN, CT, YEARS, LY, LM, FULL
    C = pd.read_csv(os.path.join(D, "carrier.csv"))
    PR = pd.read_csv(os.path.join(D, "pairs.csv"))
    CY = pd.read_csv(os.path.join(D, "cities.csv")).set_index("city")
    OT = pd.read_csv(os.path.join(D, "otp.csv"))
    DY = pd.read_csv(os.path.join(D, "daily.csv"), parse_dates=["date"])
    IN = pd.read_csv(os.path.join(D, "intl.csv"))
    CT = pd.concat(
        [
            PR.assign(city=PR.x, partner=PR.y),
            PR.assign(city=PR.y, partner=PR.x),
        ],
        ignore_index=True,
    )
    YEARS = sorted(int(value) for value in C.year.unique())
    LY = YEARS[-1]
    LM = int(C[C.year == LY].month_no.max())
    FULL = LY if LM == 12 else LY - 1


run_pipeline()
load_data()

nn=lambda v,d=0:None if v is None or pd.isna(v) else round(float(v),d)
lst=lambda s,d=0:[nn(v,d) for v in s]
yr=lambda:int(request.args.get("year",FULL))
def same(df,y):
    m=df[df.year==y].month_no.unique(); return df[(df.year==y-1)&df.month_no.isin(m)]
def gr(cur,prev): return round((float(cur)-float(prev))/float(prev)*100,1) if prev else None
def fc(s,n=6):
    y=s.values[-24:].astype(float); t=np.arange(len(y)); b,a=np.polyfit(t,y,1); res=y-(a+b*t)
    f=[max(0,a+b*(24+i)+res[12+i]) for i in range(n)]; band=1.28*res.std()
    return [int(v) for v in f],[int(max(0,v-band)) for v in f],[int(v+band) for v in f]
@app.route("/")
def home(): return send_from_directory("static","index.html")
@app.route("/api/meta")
def meta():
    a=C.groupby("airline").agg(pax=("pax","sum"),first=("year","min"),last=("year","max")).sort_values("pax",ascending=False)
    ct=CT.groupby("city").pax.sum().sort_values(ascending=False)
    return jsonify(years=YEARS,full=FULL,ly=LY,lm=LM,
      airlines=[dict(name=k,first=int(r["first"]),last=int(r["last"])) for k,r in a.iterrows()],cities=list(ct.index))
@app.route("/api/overview")
def overview():
    y=yr(); cy=C[C.year==y]; pv=same(C,y)
    yy=C.groupby("year")[["pax","flights"]].sum().reindex(YEARS); mo=cy.groupby("month_no").pax.sum().reindex(range(1,13))
    sh=cy.groupby("airline").pax.sum().sort_values(ascending=False); oth=sh.iloc[6:].sum()
    ct=CT[CT.year==y].groupby("city").pax.sum().nlargest(10); ic=IN.groupby("year").pax.sum()
    return jsonify(year=y,pax=int(cy.pax.sum()),flights=int(cy.flights.sum()),growth=gr(cy.pax.sum(),pv.pax.sum()),
      airlines=int(cy.airline.nunique()),cities=int(CT[CT.year==y].city.nunique()),routes=int(PR[PR.year==y][["x","y"]].drop_duplicates().shape[0]),
      years=YEARS,ypax=lst(yy.pax),yfl=lst(yy.flights),months=lst(mo),
      share=[[k,int(v)] for k,v in sh.head(6).items()]+([["Others",int(oth)]] if oth>0 else []),
      airports=[[k,int(v)] for k,v in ct.items()],iy=[int(k) for k in ic.index],ipax=[int(v) for v in ic.values])
@app.route("/api/shares")
def shares():
    t=C.groupby("airline").pax.sum().nlargest(8).index.tolist()
    p=C.pivot_table(index="year",columns="airline",values="pax",aggfunc="sum").reindex(YEARS).fillna(0); tot=p.sum(axis=1)
    out={k:lst(p[k]/tot*100,1) for k in t}; out["Others"]=lst(p.drop(columns=t).sum(axis=1)/tot*100,1)
    return jsonify(years=YEARS,series=out)
@app.route("/api/airline")
def airline():
    n=request.args["name"]; a=C[C.airline==n]
    if a.empty: return jsonify(error="Unknown airline"),404
    y=yr()
    if a[a.year==y].pax.sum()==0: y=int(a.year.max())
    g=a.groupby("year").agg(pax=("pax","sum"),plf=("plf","mean")).reindex(YEARS); tot=C.groupby("year").pax.sum().reindex(YEARS)
    cy=a[a.year==y]; pv=same(a,y); mo=a.groupby("month").pax.sum().sort_index()
    return jsonify(name=n,years=YEARS,pax=lst(g.pax),plf=lst(g.plf,1),share=lst(g.pax/tot*100,2),mm=list(mo.index),mp=[int(v) for v in mo.values],
      kpi=dict(year=y,pax=int(cy.pax.sum()),flights=int(cy.flights.sum()),share=nn(cy.pax.sum()/C[C.year==y].pax.sum()*100,1),
      plf=nn(cy.plf.mean(),1),growth=gr(cy.pax.sum(),pv.pax.sum()),first=int(a.year.min()),last=int(a.year.max())))
@app.route("/api/airports")
def airports():
    y=yr(); d=CT[CT.year==y]; cur=d.groupby("city").pax.sum().sort_values(ascending=False); prv=same(CT,y).groupby("city").pax.sum()
    top=d.groupby(["city","partner"]).pax.sum().reset_index().sort_values("pax").groupby("city").tail(1).set_index("city").partner
    return jsonify([dict(city=c,pax=int(v),lat=nn(CY.lat.get(c),4),lon=nn(CY.lon.get(c),4),top=top.get(c),growth=gr(v,prv.get(c,0))) for c,v in cur.items()])
@app.route("/api/airport")
def airport():
    c=request.args["city"]; a=CT[CT.city==c]
    if a.empty: return jsonify(error="Unknown airport"),404
    y=yr(); cy=a[a.year==y]; pv=same(a,y); g=a.groupby("year").pax.sum().reindex(YEARS); mo=a.groupby("month").pax.sum().sort_index()
    rk=CT[CT.year==y].groupby("city").pax.sum().rank(ascending=False).get(c)
    return jsonify(city=c,years=YEARS,pax=lst(g),mm=list(mo.index),mp=[int(v) for v in mo.values],
      partners=[[k,int(v)] for k,v in cy.groupby("partner").pax.sum().nlargest(8).items()],
      kpi=dict(pax=int(cy.pax.sum()),freight=nn(cy.freight.sum()),dest=int(cy.partner.nunique()),growth=gr(cy.pax.sum(),pv.pax.sum()),rank=nn(rk)))
@app.route("/api/routes")
def routes():
    d=PR[PR.year==yr()].groupby(["x","y"],as_index=False).pax.sum().nlargest(int(request.args.get("limit",25)),"pax")
    return jsonify([dict(a=r.x,b=r.y,pax=int(r.pax)) for r in d.itertuples()])
@app.route("/api/route")
def route():
    a,b=sorted([request.args["a"],request.args["b"]]); r=PR[(PR.x==a)&(PR.y==b)]
    if r.empty: return jsonify(error="No traffic recorded between these two cities."),404
    y=yr(); cy=r[r.year==y]; pv=same(r,y); g=r.groupby("year").pax.sum().reindex(YEARS); mo=r.groupby("month").pax.sum().sort_index()
    rk=PR[PR.year==y].groupby(["x","y"]).pax.sum().rank(ascending=False).get((a,b)); days=365 if (y!=LY or LM==12) else LM*30.4
    return jsonify(a=a,b=b,years=YEARS,pax=lst(g),mm=list(mo.index),mp=[int(v) for v in mo.values],
      kpi=dict(pax=int(cy.pax.sum()),freight=nn(cy.freight.sum()),growth=gr(cy.pax.sum(),pv.pax.sum()),rank=nn(rk),daily=int(cy.pax.sum()/days)))
@app.route("/api/reliability")
def reliability():
    mo=sorted(OT.month.unique())
    def piv(col): p=OT.pivot_table(index="month",columns="airline",values=col).reindex(mo); return {k:lst(p[k],1) for k in p.columns if p[k].notna().any()}
    last=OT[OT.month.isin(mo[-3:])].groupby("airline").otp.mean().dropna().sort_values(ascending=False)
    wd=DY.assign(w=DY.date.dt.dayofweek).groupby("w").agg(f=("dep_flights","mean")).reindex(range(7))
    return jsonify(months=mo,otp=piv("otp"),plf=piv("plf"),board=[[k,round(float(v),1)] for k,v in last.items()],wd=lst(wd.f),window=mo[-3:])
@app.route("/api/forecast")
def forecast():
    k,n=request.args.get("kind","total"),request.args.get("name","")
    df=C if k=="total" else C[C.airline==n] if k=="airline" else CT[CT.city==n]
    s=df.groupby("month").pax.sum().sort_index()
    if len(s)<24: return jsonify(error="Not enough history to forecast this series."),400
    f,lo,hi=fc(s); fm=[str(pd.Period(s.index[-1],"M")+i+1) for i in range(6)]
    return jsonify(months=list(s.index[-36:]),hist=[int(v) for v in s.values[-36:]],fmonths=fm,forecast=f,lo=lo,hi=hi)
if __name__=="__main__": app.run(debug=True,port=5000,use_reloader=False)
