"""Cleans the raw DGCA/MoCA CSVs (data/raw) into tidy files (data/processed) used by the website.
Source: https://github.com/Vonter/india-aviation-traffic"""
import pandas as pd, numpy as np, re, os
from pipeline.airport_aliases import norm
RAW,OUT="data/raw/","data/processed/"
COORD=dict((k,tuple(map(float,v.split(",")))) for k,v in (x.split(":") for x in """DELHI:28.56,77.10;MUMBAI:19.09,72.87;BENGALURU:13.20,77.71;HYDERABAD:17.24,78.43;KOLKATA:22.65,88.45;CHENNAI:12.99,80.17;AHMEDABAD:23.07,72.63;PUNE:18.58,73.92;GUWAHATI:26.11,91.59;KOCHI:10.15,76.40;LUCKNOW:26.76,80.88;GOA:15.38,73.83;JAIPUR:26.82,75.81;BHUBANESWAR:20.24,85.82;PATNA:25.59,85.09;SRINAGAR:33.99,74.77;INDORE:22.72,75.80;CHANDIGARH:30.67,76.79;VARANASI:25.45,82.86;BAGDOGRA:26.68,88.33;COIMBATORE:11.03,77.04;NAGPUR:21.09,79.05;VISAKHAPATNAM:17.72,83.22;RANCHI:23.31,85.32;RAIPUR:21.18,81.74;THIRUVANANTHAPURAM:8.48,76.92;AMRITSAR:31.71,74.80;JAMMU:32.69,74.84;PORT BLAIR:11.64,92.73;AGARTALA:23.89,91.24;UDAIPUR:24.62,73.90;MANGALORE:12.96,74.89;IMPHAL:24.76,93.90;BHOPAL:23.29,77.34;VADODARA:22.34,73.23;SURAT:21.11,72.74;MADURAI:9.83,78.09;VIJAYAWADA:16.53,80.80;LEH:34.14,77.55;DEHRADUN:30.19,78.18;RAJKOT:22.31,70.78;TIRUPATI:13.63,79.54;JODHPUR:26.25,73.05;KOZHIKODE:11.14,75.95;HUBLI:15.36,75.08;GORAKHPUR:26.74,83.45;DIBRUGARH:27.48,95.02;AURANGABAD:19.86,75.40;BELGAUM:15.86,74.62;JABALPUR:23.18,80.05;KANNUR:11.92,75.55;PRAYAGRAJ:25.44,81.73;SILCHAR:24.91,92.98;DIMAPUR:25.88,93.77;TIRUCHIRAPPALLI:10.77,78.71;KOLHAPUR:16.66,74.29;RAJAHMUNDRY:17.11,81.82;GWALIOR:26.29,78.23;BHAVNAGAR:21.75,72.18;KANDLA:23.11,70.10;JAMNAGAR:22.47,70.01;BIKANER:28.07,73.21;PUDUCHERRY:11.97,79.81;SHIRDI:19.69,74.38;KADAPA:14.51,78.77;HINDON:28.70,77.45;SALEM:11.78,78.07;BAREILLY:28.42,79.45;KURNOOL:15.71,78.17;AIZAWL:23.84,92.62;JORHAT:26.73,94.18;BILASPUR:21.99,82.11;DEOGHAR:24.45,86.70;DURGAPUR:23.62,87.24;GAYA:24.74,84.95;DARBHANGA:26.19,85.92;KISHANGARH:26.60,74.81;MYSURU:12.31,76.65;SHIMLA:31.08,77.07;KULLU:31.88,77.15;PANTNAGAR:29.03,79.47;HISAR:29.18,75.75;TEZPUR:26.71,92.78;ADAMPUR:31.43,75.76;JHARSUGUDA:21.91,84.05;JAGDALPUR:19.07,82.04;NANDED:19.18,77.32;SOLAPUR:17.63,75.93;KALABURAGI:17.31,76.96;NASHIK:20.12,73.91;PORBANDAR:21.65,69.66;DIU:20.71,70.92;KESHOD:21.31,70.27;SATNA:24.56,80.85;KHAJURAHO:24.82,79.92;REWA:24.50,81.12;DAMAN:20.43,72.84;AGATTI:10.82,72.18;MUZAFFARPUR:26.12,85.37;PASIGHAT:28.07,95.34;LILABARI:27.29,94.10;JAISALMER:26.89,70.86;AKOLA:20.70,77.06;ZIRO:27.59,93.83;TUTICORIN:8.72,78.03;THOOTHUKUDI:8.72,78.03;MUNDRA:22.84,69.72;KARAIKAL:10.92,79.84;ONGOLE:15.51,80.05;AYODHYA:26.75,82.17;KANPUR:26.44,80.37;DHARAMSALA:32.16,76.26;BHUJ:23.29,69.67;AGRA:27.16,77.96;SHILLONG:25.70,91.98""".split(";")))
def pct(s): return pd.to_numeric(s.astype(str).str.replace("%","").str.strip(),errors="coerce") if s is not None else np.nan
def prepare():
    os.makedirs(OUT,exist_ok=True)
    c=pd.read_csv(RAW+"carrier.csv"); c=c[(c.Type=="ScheduledDomestic")&(~c.Airline.str.startswith("Total"))]
    c=c.rename(columns={"Passenger Number":"pax","Aircraft Number":"flights","Passenger Load Factor":"plf","Year":"year","Month":"month_no"})
    c=c[["airline" if False else "Airline","year","month_no","pax","flights","plf"]].rename(columns={"Airline":"airline"})
    c=c[c.pax.fillna(0)>0].copy(); c["month"]=c.year.astype(str)+"-"+c.month_no.astype(str).str.zfill(2)
    c.to_csv(OUT+"carrier.csv",index=False)
    t=pd.read_csv(RAW+"city.csv").fillna(0); t["a"]=t.City1.map(norm); t["b"]=t.City2.map(norm); t=t[t.a!=t.b]
    t["pax"]=t.PaxToCity2+t.PaxFromCity2; t["freight"]=t.FreightToCity2+t.FreightFromCity2
    t["x"]=np.where(t.a<t.b,t.a,t.b); t["y"]=np.where(t.a<t.b,t.b,t.a)
    p=t.groupby(["Year","Month","x","y"],as_index=False)[["pax","freight"]].sum().rename(columns={"Year":"year","Month":"month_no"})
    p=p[p.pax>0].copy(); p["month"]=p.year.astype(str)+"-"+p.month_no.astype(str).str.zfill(2); p.to_csv(OUT+"pairs.csv",index=False)
    cities=sorted(set(p.x)|set(p.y)); pd.DataFrame([(k,*COORD.get(k,(np.nan,np.nan))) for k in cities],columns=["city","lat","lon"]).to_csv(OUT+"cities.csv",index=False)
    d=pd.read_csv(RAW+"daily.csv",parse_dates=["Date"]); rows=[]
    NM={"Air Asia":"AirAsia India","Indigo":"IndiGo","Spicejet":"SpiceJet","GoAir":"Go First"}
    for col in d.columns:
        m=re.match(r"On Time Performance \((.*)\)",col)
        if m:
            k=m.group(1); r=pd.DataFrame({"date":d.Date,"airline":NM.get(k,k),"otp":pct(d[col]),"plf":pct(d.get(f"Passenger Load Factor ({k})"))}).dropna(subset=["otp"]); rows.append(r)
    o=pd.concat(rows); o["month"]=o.date.dt.strftime("%Y-%m")
    o.groupby(["month","airline"],as_index=False)[["otp","plf"]].mean().to_csv(OUT+"otp.csv",index=False)
    pd.DataFrame({"date":d.Date,"dep_flights":d["Domestic (Departure Flights)"],"pax":d["Domestic (Arriving Pax)"]+d["Domestic (Departing Pax)"]}).dropna(subset=["dep_flights"]).to_csv(OUT+"daily.csv",index=False)
    i=pd.read_csv(RAW+"intl_country.csv"); i["year"]=2000+i.Year; i["pax"]=i.PaxToIndia+i.PaxFromIndia
    i[["year","Quarter","Country","pax"]].rename(columns={"Quarter":"quarter","Country":"country"}).to_csv(OUT+"intl.csv",index=False)
    un=p.assign(c=p.x).groupby("c").pax.sum().add(p.groupby("y").pax.sum(),fill_value=0)
    print("Unmapped (no coordinates):",[k for k in un.sort_values(ascending=False).index if k not in COORD][:15])
if __name__=="__main__": prepare(); print("Processed files written to",OUT)
