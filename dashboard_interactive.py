#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from sqlalchemy import func, and_
import sys
sys.path.insert(0, '.')

from scripts.database import SessionLocal
from scripts.models import Ciudad, RegistroClima

st.set_page_config(
    page_title="Dashboard Interactivo",
    page_icon="🎛️",
    layout="wide"
)

# CSS personalizado
st.markdown("""
    
    .metric-box {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    
""", unsafe_allow_html=True)

st.title("🎛️ Dashboard Interactivo - Control Total")

db = SessionLocal()

# Sidebar con controles
st.sidebar.markdown("### 🔧 Controles")

# Selector de ciudades
ciudades_disponibles = [c.nombre for c in db.query(Ciudad).all()]
ciudades_seleccionadas = st.sidebar.multiselect(
    "🏙️ Ciudades a Mostrar",
    options=ciudades_disponibles,
    default=ciudades_disponibles[:2]
)

# Rango de fechas
col1, col2 = st.sidebar.columns(2)
with col1:
    fecha_inicio = st.sidebar.date_input(
        "📅 Desde:",
        value=datetime.now() - timedelta(days=30)
    )
with col2:
    fecha_fin = st.sidebar.date_input(
        "📅 Hasta:",
        value=datetime.now()
    )

# Filtros de temperatura
col1, col2 = st.sidebar.columns(2)
with col1:
    temp_min = st.sidebar.slider("🌡️ Temp Mín (°C):", -50, 50, value=-10)
with col2:
    temp_max = st.sidebar.slider("🌡️ Temp Máx (°C):", -50, 50, value=40)

# Obtén datos filtrados
registros_filtrados = db.query(
    RegistroClima,
    Ciudad.nombre,
    Ciudad.pais
).join(Ciudad).filter(
    and_(
        Ciudad.nombre.in_(ciudades_seleccionadas),
        RegistroClima.fecha_extraccion >= fecha_inicio,
        RegistroClima.fecha_extraccion <= fecha_fin,
        RegistroClima.temperatura >= temp_min,
        RegistroClima.temperatura <= temp_max
    )
).all()

# Construye DataFrame
data = []
for registro, ciudad_nombre, pais in registros_filtrados:
    data.append({
        'Ciudad': ciudad_nombre,
        'País': pais,
        'Temperatura': registro.temperatura,
        'Sensación': registro.sensacion_termica,
        'Humedad': registro.humedad,
        'Viento': registro.velocidad_viento,
        'Descripción': registro.descripcion,
        'Fecha': registro.fecha_extraccion
    })

df = pd.DataFrame(data) if data else pd.DataFrame()

if not df.empty:
    # KPIs
    st.markdown("### 📊 Indicadores Clave")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            "🌡️ Temp Max",
            f"{df['Temperatura'].max():.1f}°C"
        )
    
    with col2:
        st.metric(
            "🌡️ Temp Min",
            f"{df['Temperatura'].min():.1f}°C"
        )
    
    with col3:
        st.metric(
            "🌡️ Temp Prom",
            f"{df['Temperatura'].mean():.1f}°C"
        )
    
    with col4:
        st.metric(
            "💧 Humedad Prom",
            f"{df['Humedad'].mean():.1f}%"
        )
    
    with col5:
        st.metric(
            "💨 Viento Max",
            f"{df['Viento'].max():.1f} km/h"
        )
    
    st.markdown("---")
    
    # Gráficas interactivas
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Comparativa de Temperaturas")
        fig = px.box(
            df,
            x='Ciudad',
            y='Temperatura',
            color='Ciudad',
            title='Distribución de Temperaturas por Ciudad'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("#### Promedio de Humedad")
        humedad_ciudad = df.groupby('Ciudad')['Humedad'].mean().reset_index()
        fig = px.bar(
            humedad_ciudad,
            x='Ciudad',
            y='Humedad',
            color='Humedad',
            color_continuous_scale='Blues'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Línea temporal
    st.markdown("#### 📈 Evolución Temporal")
    
    df['Fecha'] = pd.to_datetime(df['Fecha'])
    temp_tiempo = df.groupby(['Fecha', 'Ciudad'])['Temperatura'].mean().reset_index()
    
    fig = px.line(
        temp_tiempo,
        x='Fecha',
        y='Temperatura',
        color='Ciudad',
        title='Temperatura en el Tiempo',
        markers=True
    )
    st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Tabla interactiva
    st.markdown("#### 📋 Datos Detallados")
    
    # Opciones de visualización
    col1, col2 = st.columns(2)
    
    with col1:
        mostrar_todos = st.checkbox("Mostrar todos los registros", value=False)
    
    with col2:
        columnas_mostrar = st.multiselect(
            "Columnas a mostrar:",
            df.columns.tolist(),
            default=['Ciudad', 'Temperatura', 'Humedad', 'Descripción', 'Fecha']
        )
    
    if mostrar_todos:
        st.dataframe(df[columnas_mostrar], use_container_width=True, height=600)
    else:
        st.dataframe(df[columnas_mostrar].head(20), use_container_width=True)
    
    # Descargar datos
    st.markdown("---")
    csv = df.to_csv(index=False)
    st.download_button(
        label="⬇️ Descargar datos como CSV",
        data=csv,
        file_name=f"clima_datos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )

else:
    st.warning("⚠️ No hay datos que coincidan con los filtros seleccionados")

db.close()