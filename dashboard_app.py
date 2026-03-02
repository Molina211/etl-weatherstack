#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '.')

from scripts.database import SessionLocal
from scripts.models import Ciudad, RegistroClima, MetricasETL

# Configuración de la página
st.set_page_config(
    page_title="Dashboard de Clima ETL",
    page_icon="🌡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Título principal
st.title("🌍 Dashboard de Clima - ETL Weatherstack")
st.markdown("---")

# Conecta a la base de datos
db = SessionLocal()

try:
    # Obtén todos los registros de clima
    registros = db.query(RegistroClima, Ciudad.nombre).join(
        Ciudad
    ).order_by(RegistroClima.fecha_extraccion.desc()).all()

    # Transforma en DataFrame
    data = []
    for registro, ciudad_nombre in registros:
        data.append({
            'Ciudad': ciudad_nombre,
            'Temperatura': registro.temperatura,
            'Sensación Térmica': registro.sensacion_termica,
            'Humedad': registro.humedad,
            'Viento': registro.velocidad_viento,
            'Descripción': registro.descripcion,
            'Fecha': registro.fecha_extraccion
        })

    df = pd.DataFrame(data)

    # Sidebar con filtros
    st.sidebar.title("🔧 Filtros")

    if df.empty:
        st.warning("⚠️ No hay datos en la base de datos. Ejecuta primero el extractor.")
        st.info("💡 Ejecuta: `python scripts/extractor_db.py`")
        st.stop()

    ciudades_filtro = st.sidebar.multiselect(
        "Selecciona Ciudades:",
        options=df['Ciudad'].unique(),
        default=df['Ciudad'].unique()
    )

    # Filtra datos
    df_filtrado = df[df['Ciudad'].isin(ciudades_filtro)]

    if df_filtrado.empty:
        st.warning("⚠️ No hay datos con los filtros seleccionados. Selecciona al menos una ciudad.")
        st.stop()

    # Métricas principales en columnas
    st.subheader("📈 Métricas Principales")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        temp_promedio = df_filtrado['Temperatura'].mean()
        st.metric(
            label="🌡️ Temp. Promedio",
            value=f"{temp_promedio:.1f}°C",
            delta=f"{temp_promedio - 20:.1f}°C vs esperado"
        )

    with col2:
        humedad_promedio = df_filtrado['Humedad'].mean()
        st.metric(
            label="💧 Humedad Promedio",
            value=f"{humedad_promedio:.1f}%"
        )

    with col3:
        viento_maximo = df_filtrado['Viento'].max()
        ciudad_viento_series = df_filtrado[df_filtrado['Viento'] == viento_maximo]['Ciudad'].values
        ciudad_viento = ciudad_viento_series[0] if len(ciudad_viento_series) > 0 else "N/A"
        st.metric(
            label="💨 Viento Máximo",
            value=f"{viento_maximo:.1f} km/h",
            delta=f"en {ciudad_viento}"
        )

    with col4:
        total_registros = len(df_filtrado)
        st.metric(
            label="📊 Total Registros",
            value=total_registros
        )

    st.markdown("---")

    # Gráficas
    st.subheader("📉 Visualizaciones")

    col1, col2 = st.columns(2)

    # Gráfica 1: Temperatura por Ciudad
    with col1:
        fig_temp = px.bar(
            df_filtrado.sort_values('Temperatura', ascending=False),
            x='Ciudad',
            y='Temperatura',
            title="Temperatura Actual por Ciudad",
            color='Temperatura',
            color_continuous_scale='RdYlBu_r'
        )
        st.plotly_chart(fig_temp, use_container_width=True)

    # Gráfica 2: Humedad por Ciudad
    with col2:
        fig_humid = px.bar(
            df_filtrado,
            x='Ciudad',
            y='Humedad',
            title="Humedad Relativa por Ciudad",
            color='Humedad',
            color_continuous_scale='Blues'
        )
        st.plotly_chart(fig_humid, use_container_width=True)

    # Gráfica 3: Scatter Temperatura vs Humedad
    col1, col2 = st.columns(2)

    with col1:
        fig_scatter = px.scatter(
            df_filtrado,
            x='Temperatura',
            y='Humedad',
            size='Viento',
            color='Ciudad',
            title="Temperatura vs Humedad",
            hover_data=['Descripción']
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

    # Gráfica 4: Velocidad del Viento
    with col2:
        fig_wind = px.bar(
            df_filtrado.sort_values('Viento', ascending=False),
            x='Ciudad',
            y='Viento',
            title="Velocidad del Viento",
            color='Viento',
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig_wind, use_container_width=True)

    st.markdown("---")

    # Tabla de datos detallada
    st.subheader("📋 Datos Detallados")
    st.dataframe(
        df_filtrado.sort_values('Fecha', ascending=False),
        use_container_width=True,
        height=400
    )

finally:
    db.close()