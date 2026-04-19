import streamlit as st
import pandas as pd
import pydeck as pdk
import os
import sys
import types
from datetime import datetime, timedelta
from packaging.version import Version
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from engine import get_live_weather


distutils_version = types.ModuleType('distutils.version')
distutils_version.LooseVersion = Version
sys.modules['distutils.version'] = distutils_version

st.set_page_config(page_title="Ceres Smart Yield", layout="wide")

@st.cache_resource
def get_spark():
    builder = SparkSession.builder.appName("Dashboard") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .master("local[*]")
    return configure_spark_with_delta_pip(builder).getOrCreate()

@st.cache_data
def load_data():
    try:
        spark = get_spark()
        df = spark.read.format("delta").load("data/gold/farm_daily_stats")
        pdf = df.toPandas()
        pdf['date'] = pd.to_datetime(pdf['date']).dt.date
        pdf['lat'] = pd.to_numeric(pdf['lat'])
        pdf['lon'] = pd.to_numeric(pdf['lon'])
        return pdf
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

df_pd = load_data()


st.sidebar.header("Explore Data")
today = datetime.now().date()


date_range = [today + timedelta(days=i) for i in range(-10, 11)]
selected_date = st.sidebar.select_slider("Select Timeline", options=date_range, value=today)

crop_options = ["All"] + sorted(df_pd['label'].unique().tolist()) if not df_pd.empty else ["All"]
selected_crop = st.sidebar.selectbox("Select Crop", crop_options)


selected_date_str = str(selected_date)
df_filtered = df_pd[df_pd['date'] == selected_date].copy() if not df_pd.empty else pd.DataFrame()
if selected_crop != "All":
    df_filtered = df_filtered[df_filtered['label'] == selected_crop].copy()


st.title(f"🌾 Ceres Analytics: {selected_date}")


if selected_date > today:
    st.info("🔮 **Forecast Mode:** Historical data is not available for future dates.")
    st.warning("Future climate simulations are currently being computed.")
elif df_filtered.empty:
    st.warning("No data found for this selection. Please ensure the data pipeline has processed this date.")
else:
    
    avg_lat, avg_lon = df_filtered['lat'].mean(), df_filtered['lon'].mean()
    
    
    if selected_date == today:
        weather = get_live_weather(avg_lat, avg_lon, os.getenv("OPENWEATHER_API_KEY"))
        status = "Live"
    else:
        weather = {
            "main": {"temp": df_filtered['avg_temp'].mean(), "humidity": df_filtered['avg_humidity'].mean()},
            "weather": [{"description": "Historical Record"}]
        }
        status = "Historical"
    
    st.subheader(f"Regional Weather ({status})")
    c1, c2, c3 = st.columns(3)
    c1.metric("Temperature", f"{round(weather.get('main', {}).get('temp', 0), 1)}°C")
    c2.metric("Humidity", f"{round(weather.get('main', {}).get('humidity', 0), 1)}%")
    c3.metric("Conditions", weather.get('weather', [{}])[0].get('description', 'N/A'))

    
    st.subheader("Soil & Nutrient Analysis")
    col1, col2 = st.columns(2)
    with col1:
        st.write("### N-P-K Ratios")
        st.bar_chart(df_filtered[['avg_n', 'avg_p', 'avg_k']])
    with col2:
        st.write("### Soil pH Distribution")
        st.bar_chart(df_filtered[['avg_ph']])


    st.subheader("Farm Geospatial View")
    def get_crop_color(crop_name):
        color_map = {"rice": [255, 99, 71], "maize": [255, 215, 0], "chickpea": [50, 205, 50]}
        return color_map.get(str(crop_name).lower(), [200, 200, 200])

    df_filtered['color'] = df_filtered['label'].apply(get_crop_color)
    layer = pdk.Layer(
        'ScatterplotLayer', df_filtered, get_position='[lon, lat]',
        get_fill_color='color', get_radius=8000, pickable=True
    )
    view_state = pdk.ViewState(latitude=df_filtered['lat'].mean(), longitude=df_filtered['lon'].mean(), zoom=4)
    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip={"text": "Crop: {label}\nFarm: {farm_id}"}))