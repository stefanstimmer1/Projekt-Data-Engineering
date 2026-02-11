import os, psycopg2
import pandas as pd
import streamlit as st

PG_DSN = os.getenv("PG_DSN", "postgresql://test:test@postgres:5432/weather_data")

st.set_page_config(page_title="Weather Dashboard", layout="wide")
st.title("Weather Aggregates Dashboard")
st.sidebar.header("Filter")

@st.cache_data
def load_data():
    conn = psycopg2.connect(PG_DSN)
    df = pd.read_sql("""
        SELECT
          day,
          location,
          avg_temperature_c,
          avg_humidity_pct,
          sum_precipitation_mm,
          avg_wind_speed_kmh
        FROM daily_weather_aggregates
        ORDER BY day
    """, conn)
    conn.close()
    return df

df = load_data()
locations = sorted(df["location"].unique())
location = st.sidebar.selectbox("Location", locations)
df_loc = df[df["location"] == location]

# averages
col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Ø Temperature (°C)",
    round(df_loc["avg_temperature_c"].mean(), 2)
)
col2.metric(
    "Ø Humidity (%)",
    round(df_loc["avg_humidity_pct"].mean(), 2)
)
col3.metric(
    "Total precipitation (mm)",
    round(df_loc["sum_precipitation_mm"].sum(), 2)
)
col4.metric(
    "Ø Wind speed (km/h)",
    round(df_loc["avg_wind_speed_kmh"].mean(), 2)
)

# charts
st.subheader(f"Temperature - {location}")
st.line_chart(
    df_loc.set_index("day")[["avg_temperature_c"]]
)

st.subheader(f"Humidity - {location}")
st.line_chart(
    df_loc.set_index("day")[["avg_humidity_pct"]]
)

st.subheader(f"Precipitation - {location}")
st.bar_chart(
    df_loc.set_index("day")[["sum_precipitation_mm"]]
)

st.subheader(f"Wind speed - {location}")
st.bar_chart(
    df_loc.set_index("day")[["avg_wind_speed_kmh"]]
)

st.subheader("Aggregated data")
st.dataframe(df_loc, width="stretch")
