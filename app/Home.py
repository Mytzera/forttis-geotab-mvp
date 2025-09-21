import pandas as pd
import pytz
import streamlit as st
from pathlib import Path
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, timezone
import pydeck as pdk
import plotly.express as px
from PIL import Image
from config import get_api  # precisa existir na raiz do projeto

# ====== Config & helpers ======
st.set_page_config(page_title="Forttis • Geotab MVP", layout="wide")

TZ_SP = pytz.timezone("America/Sao_Paulo")

logo_path = Path(__file__).parent / "assets" / "forttis_logo.png"
if logo_path.exists():
    st.image(str(logo_path), width=180)
st.markdown("## MVP Telemetria – Forttis 🚛📊")
st.title("Hello, Forttis 👋 — MVP Geotab")
st.caption("Selecione veículo e período para visualizar rota e velocidade.")

@st.cache_resource
def get_engine():
    db_path = Path(__file__).resolve().parents[1] / "forttis.db"
    return create_engine(f"sqlite:///{db_path}")

ENGINE = get_engine()

@st.cache_data(ttl=60)
def load_devices_df():
    return pd.read_sql("SELECT id, name FROM devices ORDER BY name", ENGINE)

@st.cache_data(ttl=60)
def load_points_df(device_label, dt_ini_utc, dt_fim_utc):
    q = text("""
        SELECT device_id, date_time, latitude, longitude, speed
        FROM log_records
        WHERE device_id = :did
          AND date_time BETWEEN :dt_from AND :dt_to
        ORDER BY date_time ASC
    """)
    return pd.read_sql(q, ENGINE, params={
        "did": device_label,
        "dt_from": dt_ini_utc.isoformat(),
        "dt_to": dt_fim_utc.isoformat()
    })

@st.cache_data(ttl=60)
def load_km_period(dt_ini_utc, dt_fim_utc, only_device_id: str | None):
    """
    Calcula km rodados no período: (max(odômetro) - min(odômetro)).
    Se only_device_id=None, agrega por veículo e também retorna total da frota.
    """
    base = """
        SELECT device_id,
               MIN(odometer_km) AS odo_min,
               MAX(odometer_km) AS odo_max
        FROM odometer_samples
        WHERE date_time BETWEEN :dt_from AND :dt_to
    """
    params = {"dt_from": dt_ini_utc.isoformat(), "dt_to": dt_fim_utc.isoformat()}
    if only_device_id:
        base += " AND device_id = :did"
        params["did"] = only_device_id
    base += " GROUP BY device_id"
    df = pd.read_sql(text(base), ENGINE, params=params)
    if df.empty:
        return df, 0.0
    df["km_periodo"] = (df["odo_max"] - df["odo_min"]).clip(lower=0)
    total = float(df["km_periodo"].sum())
    return df[["device_id","km_periodo"]], total

@st.cache_data(ttl=60)
def load_incidents_df(dt_ini_utc, dt_fim_utc, only_device_id: str | None):
    base_sql = """
        SELECT e.device_id, e.rule_name, e.severity, e.date_time, d.name AS device_name
        FROM exception_events e
        JOIN devices d ON d.id = e.device_id
        WHERE e.date_time BETWEEN :dt_from AND :dt_to
          AND e.rule_name IN ('Harsh Braking','Harsh Acceleration','Harsh Cornering','Possible Collision')
    """
    params = {"dt_from": dt_ini_utc.isoformat(), "dt_to": dt_fim_utc.isoformat()}
    if only_device_id:
        base_sql += " AND e.device_id = :did"
        params["did"] = only_device_id
    base_sql += " ORDER BY e.date_time ASC"
    df = pd.read_sql(text(base_sql), ENGINE, params=params)
    if not df.empty:
        df["dt_utc"] = pd.to_datetime(df["date_time"]).dt.tz_localize("UTC")
        df["dt_sp"] = df["dt_utc"].dt.tz_convert(TZ_SP)
        # cor por severidade (RGB)
        sev_map = {
            "Critical": [200, 0, 0],
            "High":     [255, 140, 0],
            "Medium":   [255, 200, 0],
            "Low":      [120, 180, 255],
        }
        df["color"] = df["severity"].map(sev_map).fillna([150,150,150])
    return df

def fetch_feed(api, type_name: str, from_version: str | None = None, results_limit: int = 1000):
    """Wrapper compatível para GetFeed em qualquer versão do SDK."""
    params = {"typeName": type_name, "resultsLimit": results_limit}
    if from_version:
        params["fromVersion"] = from_version
    return api.call("GetFeed", **params)

# ====== Sidebar (filtros + sincronização) ======
with st.sidebar:
    st.header("Filtros")

    devices = load_devices_df()
    if devices.empty:
        st.warning("Nenhum device no banco. Rode o ETL (save_device_logs) primeiro.")
        st.stop()

    device_label = st.selectbox(
        "Veículo",
        options=devices["id"],
        format_func=lambda did: f"{devices.loc[devices['id']==did, 'name'].values[0]} ({did})",
    )

    hoje = datetime.now(tz=timezone.utc).astimezone(TZ_SP).date()
    data_ini = st.date_input("De", hoje - timedelta(days=7))
    data_fim = st.date_input("Até", hoje)
    if data_ini > data_fim:
        st.error("Data inicial maior que final.")
        st.stop()

    # escopo dos incidentes (precisa estar ANTES de usar)
    scope_inc = st.radio(
        "Escopo dos incidentes",
        options=["Somente veículo selecionado", "Toda a frota no período"],
        index=0,
    )


    

    # Botão “Sincronizar Logs”
    if st.button("🔄 Sincronizar Logs"):
        from etl.pipeline import save_logrecords
        from db.models import get_session, SyncState

        api = get_api()
        s = get_session()

        sync = s.query(SyncState).filter_by(entity="LogRecord").first()
        from_version = sync.to_version if sync else None

        feed = fetch_feed(api, "LogRecord", from_version=from_version, results_limit=1000)
        save_logrecords(feed.get("data", []))

        # atualiza token
        if sync:
            sync.to_version = feed["toVersion"]
        else:
            sync = SyncState(entity="LogRecord", to_version=feed["toVersion"])
            s.add(sync)
        s.commit()

        st.cache_data.clear()  # invalida caches de consultas
        st.success(f"{len(feed.get('data', []))} novos pontos sincronizados!")
        st.rerun()

# ====== Consulta ao SQLite com a janela selecionada ======
# (fora do sidebar)
only_this_device = device_label if scope_inc == "Somente veículo selecionado" else None

dt_ini_sp = datetime.combine(data_ini, datetime.min.time()).replace(tzinfo=TZ_SP)
dt_fim_sp = datetime.combine(data_fim, datetime.max.time()).replace(tzinfo=TZ_SP)
dt_ini_utc = dt_ini_sp.astimezone(timezone.utc)
dt_fim_utc = dt_fim_sp.astimezone(timezone.utc)

df = load_points_df(device_label, dt_ini_utc, dt_fim_utc)
inc = load_incidents_df(dt_ini_utc, dt_fim_utc, only_this_device)


km_df, km_total = load_km_period(dt_ini_utc, dt_fim_utc, only_this_device)
km_sel = float(km_df["km_periodo"].iloc[0]) if (only_this_device and not km_df.empty) else None

# cartões: adiciona mais um KPI
col1, col2, col3, col4 = st.columns(4)
# (col1..col4 já existem; vamos mostrar km no período ao lado)
if only_this_device:
    st.metric("Km no período (veículo)", f"{(km_sel or 0):,.1f}".replace(",", "."))
else:
    st.metric("Km no período (frota)", f"{km_total:,.1f}".replace(",", "."))

# KPI de incidentes graves
graves = int((inc["severity"].isin(["Critical","High"])).sum()) if not inc.empty else 0
st.metric("Incidentes graves (High/Critical)", f"{graves}")


def taxa_por_100km(num_inc, km):
    if not km or km <= 0:
        return None
    return (num_inc / km) * 100.0

if only_this_device:
    taxa = taxa_por_100km(graves, km_sel)
    if taxa is not None:
        st.metric("Incidentes graves por 100 km (veículo)", f"{taxa:.2f}")
else:
    # frota: somatório graves / km_total
    taxa = taxa_por_100km(graves, km_total)
    if taxa is not None:
        st.metric("Incidentes graves por 100 km (frota)", f"{taxa:.2f}")



st.subheader("Ranking — Menor taxa de incidentes graves por 100 km (período)")
# 1) incidentes graves por device no período
inc_g = inc[inc["severity"].isin(["High","Critical"])].copy()
if inc_g.empty or km_df.empty:
    st.info("Sem dados suficientes para calcular taxas por 100 km (verifique incidentes e odômetro).")
else:
    by_dev = inc_g.groupby("device_id").size().reset_index(name="graves")
    # 2) junta com km_df
    m = by_dev.merge(km_df, on="device_id", how="left")
    m["taxa_100km"] = m.apply(lambda r: taxa_por_100km(r["graves"], r["km_periodo"]), axis=1)
    # remove NAs e negativos
    m = m.dropna(subset=["taxa_100km"])
    # junta nomes
    names = pd.read_sql("SELECT id, name FROM devices", ENGINE)
    m = m.merge(names, left_on="device_id", right_on="id", how="left")
    m = m.sort_values(["taxa_100km","name"], ascending=[True, True])
    if m.empty:
        st.info("Sem taxas calculáveis (falta odômetro ou incidentes).")
    else:
        fig_tax = px.bar(m.head(10), x="name", y="taxa_100km",
                         labels={"name":"Veículo", "taxa_100km":"Incidentes graves / 100 km"},
                         title="Top 10 • Menor taxa (quanto menor, melhor)")
        st.plotly_chart(fig_tax, use_container_width=True)



# ====== KPIs ======
st.subheader("Resumo")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Pontos no período", f"{len(df):,}".replace(",", "."))
if not df.empty:
    first_utc = pd.to_datetime(df["date_time"].iloc[0]).tz_localize("UTC")
    last_utc  = pd.to_datetime(df["date_time"].iloc[-1]).tz_localize("UTC")
    col2.metric("Início (SP)", first_utc.tz_convert(TZ_SP).strftime("%d/%m %H:%M"))
    col3.metric("Fim (SP)",    last_utc.tz_convert(TZ_SP).strftime("%d/%m %H:%M"))
    dur = (last_utc - first_utc)
    horas = int(dur.total_seconds() // 3600); mins = int((dur.total_seconds() % 3600) // 60)
    col4.metric("Tempo de rota (aprox.)", f"{horas}h {mins}m")
else:
    col2.metric("Início (SP)", "-")
    col3.metric("Fim (SP)", "-")
    col4.metric("Tempo de rota (aprox.)", "-")
    st.info("Sem pontos nesse período. Aumente a janela ou escolha outro veículo.")
    st.stop()

# ====== Prep dados (SP) ======
df["dt_utc"] = pd.to_datetime(df["date_time"]).dt.tz_localize("UTC")
df["dt_sp"] = df["dt_utc"].dt.tz_convert(TZ_SP)
df["lat"] = df["latitude"].astype(float)
df["lon"] = df["longitude"].astype(float)

# ====== Mapa ======
st.subheader("Mapa de Rotas")
mid_lat, mid_lon = df["lat"].mean(), df["lon"].mean()
layer_pts = pdk.Layer(
    "ScatterplotLayer",
    data=df[["lat", "lon"]],
    get_position='[lon, lat]',
    get_radius=8,
    pickable=True,
    auto_highlight=True,
)
layer_path = pdk.Layer(
    "PathLayer",
    data=[{"path": df[["lon", "lat"]].to_numpy().tolist()}],
    get_path="path",
    width_scale=2,
    width_min_pixels=2,
)
view_state = pdk.ViewState(latitude=mid_lat, longitude=mid_lon, zoom=11)
st.pydeck_chart(pdk.Deck(map_style=None, initial_view_state=view_state, layers=[layer_path, layer_pts]))

# ====== Gráfico velocidade x tempo ======
st.subheader("Velocidade x Tempo")
fig = px.line(df, x="dt_sp", y="speed", labels={"dt_sp": "Hora (SP)", "speed": "Velocidade"})
st.plotly_chart(fig, use_container_width=True)

# ====== Incidentes: por regra ======
st.subheader("Incidentes por Regra (período selecionado)")
if inc.empty:
    st.info("Sem incidentes no período/escopo selecionado.")
else:
    g_rule = inc.groupby("rule_name").size().reset_index(name="qtd").sort_values("qtd", ascending=False)
    fig_rule = px.bar(g_rule, x="rule_name", y="qtd", title="Distribuição por Regra")
    st.plotly_chart(fig_rule, use_container_width=True)

# ====== Linha do tempo de graves ======
st.subheader("Linha do tempo — Incidentes graves (High/Critical)")
if inc.empty:
    st.info("Sem incidentes graves no período.")
else:
    inc_graves = inc[inc["severity"].isin(["High","Critical"])].copy()
    if inc_graves.empty:
        st.info("Sem High/Critical no período.")
    else:
        inc_graves["dia"] = inc_graves["dt_sp"].dt.date
        g_day = inc_graves.groupby("dia").size().reset_index(name="qtd")
        fig_day = px.line(g_day, x="dia", y="qtd", markers=True, labels={"dia":"Dia (SP)", "qtd":"Qtd"})
        st.plotly_chart(fig_day, use_container_width=True)

# ====== Ranking — Menos incidentes graves (período) ======
st.subheader("Ranking — Menos incidentes graves (período selecionado)")
q_inc_rank = text("""
    SELECT d.name,
           SUM(CASE WHEN e.severity IN ('Critical','High') THEN 1 ELSE 0 END) AS graves
    FROM exception_events e
    JOIN devices d ON d.id = e.device_id
    WHERE e.date_time BETWEEN :dt_from AND :dt_to
      AND e.rule_name IN ('Harsh Braking','Harsh Acceleration','Harsh Cornering','Possible Collision')
    GROUP BY d.id
    ORDER BY graves ASC, d.name ASC
    LIMIT 10
""")
df_inc_rank = pd.read_sql(q_inc_rank, ENGINE, params={"dt_from": dt_ini_utc.isoformat(), "dt_to": dt_fim_utc.isoformat()})
if df_inc_rank.empty:
    st.info("Sem incidentes graves agregados neste período.")
else:
    fig_inc_rank = px.bar(df_inc_rank, x="name", y="graves", title="Top 10 — Menos incidentes graves (quanto menor, melhor)")
    st.plotly_chart(fig_inc_rank, use_container_width=True)

# ====== Mapa — Incidentes (match temporal com pontos) ======
st.subheader("Mapa — Incidentes no período")
if inc.empty:
    st.info("Sem incidentes para mapear.")
else:
    # pontos usados para georreferenciar incidentes:
    if only_this_device:
        pts = df.copy()
    else:
        # fallback: usa o mesmo veículo selecionado (para evitar carregar toda a frota aqui)
        pts = df.copy()
    if pts.empty:
        st.info("Sem pontos suficientes para georreferenciar os incidentes.")
    else:
        pts["dt_utc"] = pd.to_datetime(pts["date_time"]).dt.tz_localize("UTC")
        inc_map = []
        for _, row in inc.iterrows():
            dev = row["device_id"]
            t = row["dt_utc"]
            window = pts[(pts["device_id"]==dev) & (pts["dt_utc"].between(t - pd.Timedelta("5min"), t + pd.Timedelta("5min")))]
            if not window.empty:
                idx = (window["dt_utc"] - t).abs().idxmin()
                inc_map.append({
                    "lat": float(window.loc[idx,"latitude"]),
                    "lon": float(window.loc[idx,"longitude"]),
                    "sev": row["severity"],
                    "rule": row["rule_name"],
                    "ts": row["dt_sp"].strftime("%d/%m %H:%M"),
                    "color": row["color"],
                })
        if not inc_map:
            st.info("Não foi possível posicionar incidentes (sem pontos próximos no tempo).")
        else:
            layer_inc = pdk.Layer(
                "ScatterplotLayer",
                data=inc_map,
                get_position='[lon, lat]',
                get_radius=60,
                get_fill_color="color",
                pickable=True,
            )
            tooltip = {"html": "<b>{rule}</b><br/>{sev}<br/>{ts}", "style": {"backgroundColor": "steelblue", "color": "white"}}
            st.pydeck_chart(pdk.Deck(
                map_style=None,
                initial_view_state=pdk.ViewState(latitude=df["lat"].mean(), longitude=df["lon"].mean(), zoom=11),
                layers=[layer_inc],
                tooltip=tooltip
            ))

# ====== Export CSV de Incidentes ======
if not inc.empty:
    st.download_button(
        label="⚠️ Exportar CSV de Incidentes",
        data=inc.drop(columns=["dt_utc","color"]).to_csv(index=False).encode("utf-8"),
        file_name=f"incidentes_{'dev_'+device_label if only_this_device else 'frota'}_{data_ini}_{data_fim}.csv",
        mime="text/csv"
    )

# ====== Export CSV de pontos ======
st.download_button(
    label="📥 Exportar CSV (pontos)",
    data=df.to_csv(index=False).encode("utf-8"),
    file_name=f"{device_label}_{data_ini}_{data_fim}.csv",
    mime="text/csv"
)

# ====== Ranking simples por pontos (todo o banco) ======
st.subheader("Ranking de Veículos (mais pontos)")
df_rank = pd.read_sql("""
    SELECT d.name, COUNT(l.id) as pontos
    FROM log_records l
    JOIN devices d ON d.id = l.device_id
    GROUP BY d.id
    ORDER BY pontos DESC
    LIMIT 5
""", ENGINE)
fig_rank = px.bar(df_rank, x="name", y="pontos", title="Top 5 veículos por pontos coletados")
st.plotly_chart(fig_rank, use_container_width=True)
