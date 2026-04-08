import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Finsight",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

API_BASE = "http://finsight-api:8000"

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Sans:wght@300;400;500&display=swap');

/* Global */
html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
    background-color: #f7f5f2;
    color: #1a1a1a;
}

/* Hide default streamlit chrome */
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding: 2.5rem 3rem 3rem 3rem; max-width: 1100px; }

/* Hero title */
.hero-title {
    font-family: 'DM Serif Display', serif;
    font-size: 3.2rem;
    font-weight: 400;
    letter-spacing: -0.02em;
    color: #1a1a1a;
    margin-bottom: 0.2rem;
    line-height: 1.1;
}
.hero-sub {
    font-size: 1rem;
    color: #888;
    font-weight: 300;
    margin-bottom: 2.5rem;
    letter-spacing: 0.01em;
}

/* Search bar container */
.search-wrap {
    background: #ffffff;
    border: 1.5px solid #e8e4df;
    border-radius: 14px;
    padding: 1.5rem 1.8rem;
    margin-bottom: 1.5rem;
    box-shadow: 0 2px 12px rgba(0,0,0,0.04);
}

/* Streamlit input override */
div[data-testid="stTextInput"] input {
    background: #f7f5f2 !important;
    border: 1.5px solid #e0dbd4 !important;
    border-radius: 10px !important;
    font-family: 'DM Sans', sans-serif !important;
    font-size: 1.05rem !important;
    padding: 0.65rem 1rem !important;
    color: #1a1a1a !important;
    transition: border-color 0.2s;
}
div[data-testid="stTextInput"] input:focus {
    border-color: #b5a99a !important;
    box-shadow: 0 0 0 3px rgba(181,169,154,0.15) !important;
}

/* Buttons */
div[data-testid="stButton"] button {
    background: #1a1a1a !important;
    color: #f7f5f2 !important;
    border: none !important;
    border-radius: 10px !important;
    font-family: 'DM Sans', sans-serif !important;
    font-weight: 500 !important;
    font-size: 0.9rem !important;
    padding: 0.55rem 1.4rem !important;
    letter-spacing: 0.02em !important;
    transition: background 0.2s, transform 0.1s !important;
    cursor: pointer !important;
}
div[data-testid="stButton"] button:hover {
    background: #333 !important;
    transform: translateY(-1px) !important;
}
div[data-testid="stButton"] button:active {
    transform: translateY(0px) !important;
}

/* Metric cards */
.metric-card {
    background: #ffffff;
    border: 1.5px solid #e8e4df;
    border-radius: 14px;
    padding: 1.2rem 1.5rem;
    text-align: center;
    box-shadow: 0 2px 8px rgba(0,0,0,0.03);
}
.metric-label {
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: #aaa;
    margin-bottom: 0.4rem;
    font-weight: 500;
}
.metric-value {
    font-family: 'DM Serif Display', serif;
    font-size: 1.7rem;
    color: #1a1a1a;
    line-height: 1;
}
.metric-value.positive { color: #3a7d5c; }
.metric-value.negative { color: #c0392b; }

/* Section label */
.section-label {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    color: #bbb;
    font-weight: 500;
    margin-bottom: 0.8rem;
    margin-top: 2rem;
}

/* Tag */
.ticker-tag {
    display: inline-block;
    background: #f0ede8;
    color: #555;
    font-size: 0.78rem;
    font-weight: 500;
    letter-spacing: 0.08em;
    padding: 0.2rem 0.7rem;
    border-radius: 6px;
    margin-bottom: 1rem;
    text-transform: uppercase;
}

/* Divider */
.soft-divider {
    border: none;
    border-top: 1px solid #ede9e3;
    margin: 1.8rem 0;
}

/* Risk metric row */
.risk-row {
    display: flex;
    gap: 1rem;
    margin-bottom: 1rem;
}

/* Alert box */
.info-box {
    background: #f0ede8;
    border-left: 3px solid #c8bfb4;
    border-radius: 8px;
    padding: 0.9rem 1.2rem;
    font-size: 0.9rem;
    color: #555;
    margin-top: 0.5rem;
}
</style>
""", unsafe_allow_html=True)


# ── Helpers ───────────────────────────────────────────────────────────────────
def fetch(endpoint: str, params: dict = {}):
    try:
        r = requests.get(f"{API_BASE}{endpoint}", params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API error: {e}")
        return None


def color_class(val):
    if val is None:
        return ""
    return "positive" if float(val) >= 0 else "negative"


def fmt(val, suffix="", decimals=2):
    if val is None:
        return "—"
    return f"{float(val):,.{decimals}f}{suffix}"


# ── Header ────────────────────────────────────────────────────────────────────
st.markdown('<div class="hero-title">Finsight</div>', unsafe_allow_html=True)
st.markdown('<div class="hero-sub">Real-time stock analytics · Forecast · Risk</div>', unsafe_allow_html=True)

# ── Ticker input ──────────────────────────────────────────────────────────────
with st.container():
    col_in, col_btn = st.columns([4, 1])
    with col_in:
        ticker_input = st.text_input(
            "",
            placeholder="Enter a ticker symbol — AAPL, MSFT, GOOGL, TSLA",
            label_visibility="collapsed",
            key="ticker_input",
        )
    with col_btn:
        search = st.button("Look up →", use_container_width=True)

ticker = ticker_input.strip().upper()

# ── Main content ──────────────────────────────────────────────────────────────
if ticker and search or (ticker and st.session_state.get("last_ticker") == ticker):
    st.session_state["last_ticker"] = ticker

    # ── Latest price strip ────────────────────────────────────────────────
    latest = fetch("/prices/latest", {"ticker": ticker})
    if latest:
        st.markdown(f'<div class="ticker-tag">{ticker}</div>', unsafe_allow_html=True)

        c1, c2, c3, c4 = st.columns(4)
        fields = [
            ("Close", latest.get("close"), "$"),
            ("Open",  latest.get("open"),  "$"),
            ("High",  latest.get("high"),  "$"),
            ("Volume", latest.get("volume"), ""),
        ]
        for col, (label, val, prefix) in zip([c1, c2, c3, c4], fields):
            with col:
                if label == "Volume":
                    disp = f"{int(val or 0):,}"
                else:
                    disp = f"{prefix}{fmt(val)}"
                css = color_class(val) if label == "Close" else ""
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">{label}</div>
                    <div class="metric-value {css}">{disp}</div>
                </div>""", unsafe_allow_html=True)

    st.markdown('<hr class="soft-divider">', unsafe_allow_html=True)

    # ── Price chart ───────────────────────────────────────────────────────
    tf_col, label_col = st.columns([3, 1])
    with label_col:
        st.markdown('<div class="section-label" style="text-align:right;margin-top:0">Price History</div>', unsafe_allow_html=True)
    with tf_col:
        timeframes = {"1W": 7, "1M": 30, "3M": 90, "6M": 180, "1Y": 365}
        selected_tf = st.radio(
            "Timeframe",
            options=list(timeframes.keys()),
            index=2,
            horizontal=True,
            label_visibility="collapsed",
        )

    prices_data = fetch("/prices", {"ticker": ticker, "limit": 365})
    if prices_data and prices_data.get("prices"):
        df = pd.DataFrame(prices_data["prices"])
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")

        days = timeframes[selected_tf]
        cutoff = df["date"].max() - pd.Timedelta(days=days)
        dff = df[df["date"] >= cutoff]

        fig = go.Figure()
        fig.add_trace(go.Candlestick(
            x=dff["date"],
            open=dff["open"],
            high=dff["high"],
            low=dff["low"],
            close=dff["close"],
            increasing_line_color="#3a7d5c",
            decreasing_line_color="#c0392b",
            increasing_fillcolor="#3a7d5c",
            decreasing_fillcolor="#c0392b",
            name="OHLC",
            line=dict(width=1),
        ))
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="#ffffff",
            margin=dict(l=0, r=0, t=10, b=0),
            height=360,
            xaxis=dict(
                showgrid=False, zeroline=False, color="#aaa",
                tickfont=dict(family="DM Sans", size=11),
                rangeslider=dict(visible=False),
            ),
            yaxis=dict(
                showgrid=True, gridcolor="#f0ede8", zeroline=False,
                color="#aaa", tickfont=dict(family="DM Sans", size=11),
                tickprefix="$",
            ),
            legend=dict(visible=False),
            font=dict(family="DM Sans"),
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    st.markdown('<hr class="soft-divider">', unsafe_allow_html=True)

    # ── Action buttons ────────────────────────────────────────────────────
    btn_col1, btn_col2, _ = st.columns([1, 1, 3])
    with btn_col1:
        show_forecast = st.button("7-day Forecast", use_container_width=True)
    with btn_col2:
        show_risk = st.button("Risk Metrics", use_container_width=True)

    if show_forecast:
        st.session_state["show_forecast"] = True
        st.session_state["show_risk"] = False
    if show_risk:
        st.session_state["show_risk"] = True
        st.session_state["show_forecast"] = False

    # ── Forecast ──────────────────────────────────────────────────────────
    if st.session_state.get("show_forecast"):
        st.markdown('<div class="section-label">7-Day Forecast · Linear Regression</div>', unsafe_allow_html=True)
        fc = fetch("/forecast", {"ticker": ticker})
        if fc and fc.get("forecasts"):
            fdf = pd.DataFrame(fc["forecasts"][:7])
            fdf["date"] = pd.to_datetime(fdf["forecast_date"])
            fdf = fdf.sort_values("date")

            fig2 = go.Figure()

            # Historical close for context
            if prices_data and prices_data.get("prices"):
                fig2.add_trace(go.Scatter(
                    x=df["date"], y=df["close"],
                    mode="lines",
                    line=dict(color="#c8bfb4", width=1.5),
                    name="Historical",
                ))

            # Forecast line
            fig2.add_trace(go.Scatter(
                x=fdf["date"], y=fdf["predicted_close"],
                mode="lines+markers",
                line=dict(color="#1a1a1a", width=2, dash="dot"),
                marker=dict(size=4, color="#1a1a1a"),
                name="Forecast",
            ))

            fig2.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="#ffffff",
                margin=dict(l=0, r=0, t=10, b=0),
                height=300,
                xaxis=dict(showgrid=False, zeroline=False, color="#aaa",
                           tickfont=dict(family="DM Sans", size=11)),
                yaxis=dict(showgrid=True, gridcolor="#f0ede8", zeroline=False,
                           color="#aaa", tickfont=dict(family="DM Sans", size=11),
                           tickprefix="$"),
                legend=dict(font=dict(family="DM Sans", size=11),
                            bgcolor="rgba(0,0,0,0)"),
                font=dict(family="DM Sans"),
            )
            st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})

            # Summary
            last_price = df["close"].iloc[-1] if prices_data else None
            last_forecast = fdf["predicted_close"].iloc[-1]
            if last_price:
                chg = ((last_forecast - last_price) / last_price) * 100
                arrow = "↑" if chg >= 0 else "↓"
                clr = "#3a7d5c" if chg >= 0 else "#c0392b"
                st.markdown(f"""
                <div class="info-box">
                    Model projects <strong style="color:{clr}">{arrow} {abs(chg):.1f}%</strong>
                    over 7 days · Target <strong>${last_forecast:.2f}</strong>
                </div>""", unsafe_allow_html=True)

    # ── Risk ──────────────────────────────────────────────────────────────
    if st.session_state.get("show_risk"):
        st.markdown('<div class="section-label">Risk Metrics</div>', unsafe_allow_html=True)
        risk = fetch("/risk", {"ticker": ticker})
        if risk:
            r1, r2, r3 = st.columns(3)
            sharpe = risk.get("sharpe_ratio")
            vol    = risk.get("annual_volatility")
            mdd    = risk.get("max_drawdown")

            with r1:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Sharpe Ratio</div>
                    <div class="metric-value {color_class(sharpe)}">{fmt(sharpe)}</div>
                </div>""", unsafe_allow_html=True)
            with r2:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Annual Volatility</div>
                    <div class="metric-value">{fmt(vol, "%")}</div>
                </div>""", unsafe_allow_html=True)
            with r3:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Max Drawdown</div>
                    <div class="metric-value {color_class(mdd)}">{fmt(mdd, "%")}</div>
                </div>""", unsafe_allow_html=True)

            # Guidance
            if sharpe is not None:
                s = float(sharpe)
                if s > 1:
                    note = "Sharpe > 1 suggests strong risk-adjusted returns."
                elif s > 0:
                    note = "Positive Sharpe — returns exceed the risk-free rate."
                else:
                    note = "Negative Sharpe — returns currently below risk-free rate."
                st.markdown(f'<div class="info-box" style="margin-top:1rem">{note}</div>',
                            unsafe_allow_html=True)

else:
    # Empty state
    st.markdown("""
    <div style="text-align:center; padding: 4rem 0; color: #ccc;">
        <div style="font-family:'DM Serif Display',serif; font-size:2rem; margin-bottom:0.5rem">
            Enter a ticker to begin
        </div>
        <div style="font-size:0.9rem; font-weight:300">
            Try AAPL · MSFT · GOOGL · TSLA
        </div>
    </div>
    """, unsafe_allow_html=True)

# Reset session flags when ticker changes
if ticker != st.session_state.get("last_ticker_check"):
    st.session_state["show_forecast"] = False
    st.session_state["show_risk"] = False
    st.session_state["last_ticker_check"] = ticker
