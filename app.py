import streamlit as st
import holidays
import pandas as pd
import numpy as np
import re
import io
from datetime import datetime, timedelta

# ── Configuración de página ────────────────────────────────────────────────────
st.set_page_config(
    page_title="Procesador de Horarios Docentes",
    page_icon="🏫",
    layout="wide",
)

st.title("🏫 Procesador de Horarios Docentes")
st.markdown("Sube los dos archivos Excel, configura el año y descarga el reporte consolidado.")

# ── Sidebar: Configuración ─────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Configuración")
    YEAR = st.number_input("Año de procesamiento", min_value=2020, max_value=2035, value=2026, step=1)
    BASE_FECHA = f"{YEAR}-01-01 "

    st.subheader("📅 Semana Santa")
    ss_ini = st.text_input("Inicio Semana Santa (YYYY-MM-DD)", value=f"{YEAR}-03-29")
    ss_fin = st.text_input("Fin Semana Santa (YYYY-MM-DD)",   value=f"{YEAR}-04-05")

    st.subheader("🌙 Recargo nocturno")
    recargo_ini = st.text_input("Inicio recargo", value="19:00")
    recargo_fin = st.text_input("Fin recargo",    value="22:00")

# ── Constantes derivadas ───────────────────────────────────────────────────────
MESES_ES = {
    1: "ene", 2: "feb", 3: "mar", 4: "abr", 5: "may", 6: "jun",
    7: "jul", 8: "ago", 9: "sep", 10: "oct", 11: "nov", 12: "dic"
}
DIAS_MAP = {
    "Monday": "LU", "Tuesday": "MA", "Wednesday": "MI",
    "Thursday": "JU", "Friday": "VI", "Saturday": "SA", "Sunday": "DO"
}

# ── Helpers ────────────────────────────────────────────────────────────────────
def get_semana_rango_es(row):
    """Convierte NUM_SEMANA a un rango legible en español, ej: '05 ene - 11 ene'."""
    semana_ajustada = int(row["NUM_SEMANA"]) - 1
    lunes = datetime.strptime(f'{YEAR}-W{semana_ajustada}-1', "%Y-W%W-%w")
    domingo = lunes + timedelta(days=6)
    return f"{lunes.day:02d} {MESES_ES[lunes.month]} - {domingo.day:02d} {MESES_ES[domingo.month]}"


def limpiar_horarios(texto):
    """Parsea un bloque de texto con horarios y retorna lista de dicts {dia, hora_inicio, hora_fin}."""
    if not isinstance(texto, str):
        return []
    resultados = []
    for fila in texto.split("\n"):
        fila = fila.strip().upper()
        dia = re.search(r'\b(LU|MA|MI|JU|VI|SA|DO)\b', fila)
        if not dia:
            continue
        horas = re.search(r'(\d{2}:\d{2})\s*-\s*(\d{2}:\d{2})', fila)
        if horas:
            resultados.append({
                "dia": dia.group(1),
                "hora_inicio": horas.group(1),
                "hora_fin": horas.group(2)
            })
    return resultados


def horas_entre(df, col_ini, col_fin):
    """Calcula diferencia en horas entre dos columnas de tipo string HH:MM."""
    h_ini = pd.to_datetime(BASE_FECHA + df[col_ini].astype(str), errors='coerce')
    h_fin = pd.to_datetime(BASE_FECHA + df[col_fin].astype(str), errors='coerce')
    diff = (h_fin - h_ini).dt.total_seconds() / 3600
    return np.where(diff < 0, diff + 24, diff)


def calcular_recargo(df, col_ini, col_fin, inicio=None, fin=None):
    """Calcula horas dentro del rango de recargo nocturno."""
    inicio = inicio or recargo_ini
    fin    = fin    or recargo_fin
    h_ini    = pd.to_datetime(BASE_FECHA + df[col_ini].astype(str), errors='coerce')
    h_fin    = pd.to_datetime(BASE_FECHA + df[col_fin].astype(str), errors='coerce')
    p_inicio = pd.to_datetime(BASE_FECHA + inicio)
    p_fin    = pd.to_datetime(BASE_FECHA + fin)
    diferencia = (h_fin.clip(upper=p_fin) - h_ini.clip(lower=p_inicio)).dt.total_seconds() / 3600
    return diferencia.clip(lower=0).fillna(0).round(2)


def calcular_recargos_reales(row):
    if str(row['ENTRADA_BIO']).upper() == 'SIN MARCA' or pd.isna(row['ENTRADA_BIO']):
        return 0.0
    inicio_recargo = pd.to_datetime(BASE_FECHA + recargo_ini)
    fin_recargo    = pd.to_datetime(BASE_FECHA + recargo_fin)
    try:
        real_in  = pd.to_datetime(BASE_FECHA + str(row['ENTRADA_BIO']), errors='coerce')
        real_out = pd.to_datetime(BASE_FECHA + str(row['SALIDA_BIO']),  errors='coerce')
        proj_in  = pd.to_datetime(BASE_FECHA + str(row['INI_CLASE']),   errors='coerce')
        proj_out = pd.to_datetime(BASE_FECHA + str(row['FIN_CLASE']),   errors='coerce')
        if pd.isna(real_in) or pd.isna(real_out) or pd.isna(proj_out):
            return 0.0
        if proj_out <= inicio_recargo:
            return 0.0
        inicio_v = max(real_in, proj_in, inicio_recargo)
        fin_v    = min(real_out, proj_out, fin_recargo)
        if fin_v > inicio_v:
            return round((fin_v - inicio_v).total_seconds() / 3600, 2)
    except Exception as e:
        st.warning(f"Error calculando recargo en fila: {e}")
        return 0.0
    return 0.0


# ── Procesamiento principal ────────────────────────────────────────────────────
def procesar(archivo_horarios, archivo_biometrico):
    FESTIVOS_CO = holidays.Colombia(years=YEAR)
    SEMANA_SANTA = (ss_ini, ss_fin)

    progress = st.progress(0, text="Cargando archivos...")

    # ── 1. Carga y limpieza inicial ──────────────────────────────────────────
    df = pd.read_excel(archivo_horarios)
    df["HORAS"]       = df["HORAS"].str.replace("NO TIENE", "0")
    df["MATERIA_INI"] = pd.to_datetime(df["MATERIA_INI"], dayfirst=True)
    df["MATERIA_FIN"] = pd.to_datetime(df["MATERIA_FIN"], dayfirst=True)

    cols_drop = [c for c in ['MATERIA_ACTIVIDAD', 'SEDE', 'GRUPO', 'COD_MATERIA', 'CENTRO_COSTO', 'NPLAN']
                 if c in df.columns]
    df = df.drop(columns=cols_drop)

    progress.progress(10, text="Limpieza inicial completada...")

    # ── 2. Explotar horarios por fila ───────────────────────────────────────
    df["horarios_lista"] = df["HORAS"].apply(limpiar_horarios)
    df = df.explode("horarios_lista").reset_index(drop=True)
    df_final = pd.concat([df.drop(columns=["horarios_lista"]),
                          pd.json_normalize(df["horarios_lista"])], axis=1)

    progress.progress(20, text="Horarios expandidos...")

    # ── 3. Separar clases normales vs complementarias ──────────────────────
    SIN_HORARIO = [0, "0", None, ""]
    df_clases = df_final[~df_final["HORAS"].isin(SIN_HORARIO)]
    df_compl  = df_final[ df_final["HORAS"].isin(SIN_HORARIO)]

    COLS_GRUPO = ["CODIGO", "DOCUMENTO", "NOMBRE", "MATERIA_INI", "MATERIA_FIN"]
    COLS_GRUPO = [c for c in COLS_GRUPO if c in df_compl.columns]

    df_resumen_compl = (
        df_compl.groupby(COLS_GRUPO)
                .agg(TOTAL_HORAS=("TOTAL_HORAS", "sum"))
                .reset_index()
                .assign(dia="COMPL", HORAS="TOTAL SEMANAL")
    )

    df_final = pd.concat([df_clases, df_resumen_compl], ignore_index=True)
    df_final.sort_values("NOMBRE", inplace=True)

    progress.progress(30, text="Separando clases y complementarias...")

    # ── 4. Expandir clases a fechas diarias ─────────────────────────────────
    df_para_expandir   = df_final[df_final["dia"] != "COMPL"]
    df_resumen_semanal = df_final[df_final["dia"] == "COMPL"]

    filas_expandidas = []
    for _, row in df_para_expandir.iterrows():
        temp = pd.DataFrame({"fecha": pd.date_range(row["MATERIA_INI"], row["MATERIA_FIN"], freq="D")})
        temp["dia"] = temp["fecha"].dt.day_name().map(DIAS_MAP)
        temp = temp[temp["dia"] == row["dia"]]
        for col in df_para_expandir.columns:
            if col not in ["fecha", "dia"]:
                temp[col] = row[col]
        filas_expandidas.append(temp)

    df_clases_diarias = pd.concat(filas_expandidas, ignore_index=True) if filas_expandidas else pd.DataFrame()
    df_calendario = pd.concat([df_clases_diarias, df_resumen_semanal], ignore_index=True)
    df_calendario.sort_values(["NOMBRE", "fecha"], inplace=True)
    df_calendario = df_calendario.drop(columns=["HORAS"])

    progress.progress(45, text="Calendario diario generado...")

    # ── 5. Quitar festivos y Semana Santa ────────────────────────────────────
    df_calendario = df_calendario[
        (df_calendario["fecha"] < SEMANA_SANTA[0]) |
        (df_calendario["fecha"] > SEMANA_SANTA[1]) |
        (df_calendario["dia"] == "COMPL")
    ]
    festivos_dt = pd.to_datetime(list(FESTIVOS_CO.keys()))
    df_calendario = df_calendario[
        ~df_calendario["fecha"].isin(festivos_dt) | (df_calendario["dia"] == "COMPL")
    ]

    progress.progress(55, text="Festivos y Semana Santa removidos...")

    # ── 6. Agrupar por día y calcular horas + recargo ───────────────────────
    df_calendario['duracion_clase'] = (
        pd.to_datetime(df_calendario['hora_fin'],    format='%H:%M') -
        pd.to_datetime(df_calendario['hora_inicio'], format='%H:%M')
    ).dt.total_seconds() / 3600

    df_agrupado = (
        df_calendario[df_calendario["dia"] != "COMPL"]
        .groupby(['DOCUMENTO', 'fecha'])
        .agg(
            Entrada_Real    = ('hora_inicio',    'min'),
            Salida_Real     = ('hora_fin',       'max'),
            horas_laborales = ('duracion_clase', 'sum')
        )
        .reset_index()
    )
    df_agrupado['horas_laborales']    = df_agrupado['horas_laborales'].round(2)
    df_agrupado['recargo_proyectado'] = calcular_recargo(df_agrupado, 'Entrada_Real', 'Salida_Real')

    progress.progress(65, text="Horas y recargos calculados...")

    # ── 7. Unir complementarias y construir consolidado ─────────────────────
    df_compl_final = (
        df_calendario[df_calendario["dia"] == "COMPL"]
        .copy()
        .assign(fecha=lambda x: x["MATERIA_INI"],
                Entrada_Real="00:00", Salida_Real="00:00",
                recargo_proyectado=0.0)
        .rename(columns={"TOTAL_HORAS": "horas_laborales"})
    )

    COLS_FINALES = ['DOCUMENTO', 'fecha', 'Entrada_Real', 'Salida_Real', 'horas_laborales', 'recargo_proyectado']
    df_consolidado = pd.concat([df_agrupado, df_compl_final[COLS_FINALES]], ignore_index=True)

    # ── 8. Llave y orden final ───────────────────────────────────────────────
    mask_clases = df_consolidado['Entrada_Real'] != "00:00"
    df_consolidado['llave'] = np.where(
        mask_clases,
        df_consolidado['fecha'].dt.strftime('%d/%m/%Y') + '-' + df_consolidado['DOCUMENTO'].astype(str),
        pd.NA
    )
    df_consolidado = (
        df_consolidado
        .sort_values(['DOCUMENTO', 'fecha', 'Entrada_Real'], ascending=[True, True, False])
        [['llave', 'DOCUMENTO', 'fecha', 'Entrada_Real', 'Salida_Real', 'horas_laborales', 'recargo_proyectado']]
    )

    progress.progress(70, text="Cruzando con biométrico...")

    # ── CRUCE BIOMÉTRICO ─────────────────────────────────────────────────────
    biometrico = pd.read_excel(
        archivo_biometrico,
        usecols=["fecha", "Documento", "hora_entrada", "hora_salida", "horas"],
        skiprows=1
    )
    biometrico.columns = biometrico.columns.str.upper().str.strip()
    biometrico['FECHA'] = pd.to_datetime(biometrico['FECHA'], dayfirst=True, errors='coerce')

    biometrico_unificado = biometrico.groupby(['FECHA', 'DOCUMENTO']).agg(
        HORA_ENTRADA=('HORA_ENTRADA', 'min'),
        HORA_SALIDA =('HORA_SALIDA',  'max'),
        HORAS       =('HORAS',        'sum')
    ).reset_index()

    llave_col = biometrico_unificado['FECHA'].dt.strftime('%d/%m/%Y') + '-' + biometrico_unificado['DOCUMENTO'].astype(str)
    biometrico_unificado.insert(0, 'LLAVE', llave_col)
    biometrico_unificado['NUM_SEMANA'] = biometrico_unificado['FECHA'].dt.isocalendar().week

    # Cruce con consolidado
    df_detalle_final = pd.merge(
        df_consolidado,
        biometrico_unificado[['LLAVE', 'HORA_ENTRADA', 'HORA_SALIDA', 'HORAS']],
        left_on='llave', right_on='LLAVE',
        how='left'
    ).drop(columns=['LLAVE'])

    df_detalle_final.rename(columns={
        'Entrada_Real':   'INI_CLASE',
        'Salida_Real':    'FIN_CLASE',
        'horas_laborales':'HORAS_CLASE',
        'HORA_ENTRADA':   'ENTRADA_BIO',
        'HORA_SALIDA':    'SALIDA_BIO',
        'HORAS':          'TOTAL_BIO_DIA'
    }, inplace=True)

    cols = ['llave'] + [c for c in df_detalle_final.columns if c != 'llave']
    df_detalle_final = df_detalle_final[cols]

    progress.progress(80, text="Calculando recargos reales...")

    # ── Recargos nocturnos reales ────────────────────────────────────────────
    df_detalle_final['TOTAL_HORAS_RECARGO'] = 0.0
    mask_recargo = df_detalle_final['recargo_proyectado'] > 0
    df_detalle_final.loc[mask_recargo, 'TOTAL_HORAS_RECARGO'] = (
        df_detalle_final[mask_recargo].apply(calcular_recargos_reales, axis=1)
    )

    # ── Semana y agrupado semanal ────────────────────────────────────────────
    df_detalle_final['NUM_SEMANA'] = pd.to_datetime(df_detalle_final['fecha']).dt.isocalendar().week

    biometrico_semanal = (
        df_detalle_final
        .groupby(['DOCUMENTO', 'NUM_SEMANA'])
        .agg(
            TOTAL_BIO_SEMANA  =('TOTAL_BIO_DIA',       'sum'),
            TOTAL_RECARGO_SEM =('TOTAL_HORAS_RECARGO',  'sum')
        )
        .reset_index()
    )

    progress.progress(90, text="Construyendo resumen semanal...")

    # ── BALANCE FINAL ────────────────────────────────────────────────────────
    clases_fijas = df_consolidado[df_consolidado["llave"].notna()].copy()
    clases_fijas["NUM_SEMANA"] = clases_fijas["fecha"].dt.isocalendar().week

    compl_base = df_calendario[df_calendario["dia"] == "COMPL"].copy()
    filas_repartidas = []
    for _, row in compl_base.iterrows():
        for fecha_lunes in pd.date_range(start=row["MATERIA_INI"], end=row["MATERIA_FIN"], freq="W-MON"):
            nueva_fila = row.copy()
            nueva_fila["NUM_SEMANA"] = fecha_lunes.isocalendar().week
            filas_repartidas.append(nueva_fila)
    df_compl_por_semana = pd.DataFrame(filas_repartidas)

    clases_para_unir = clases_fijas[['DOCUMENTO', 'NUM_SEMANA', 'horas_laborales', 'recargo_proyectado']].rename(
        columns={'horas_laborales': 'HORAS_VALOR', 'recargo_proyectado': 'RECARGOS_VALOR'})
    compl_para_unir = df_compl_por_semana[['DOCUMENTO', 'NUM_SEMANA', 'TOTAL_HORAS']].rename(
        columns={'TOTAL_HORAS': 'HORAS_VALOR'})
    compl_para_unir['RECARGOS_VALOR'] = 0.0

    df_consolidado_total = pd.concat([clases_para_unir, compl_para_unir], ignore_index=True)

    resumen_final = (
        df_consolidado_total
        .groupby(['DOCUMENTO', 'NUM_SEMANA'])
        .agg(
            HORAS_VALOR    =('HORAS_VALOR',    'sum'),
            RECARGOS_VALOR =('RECARGOS_VALOR', 'sum')
        )
        .reset_index()
        .rename(columns={
            'HORAS_VALOR':    'HORAS QUE DEBIA HACER',
            'RECARGOS_VALOR': 'RECARGOS QUE DEBIA HACER'
        })
    )

    resumen_final = pd.merge(
        resumen_final,
        biometrico_semanal,
        on=['DOCUMENTO', 'NUM_SEMANA'],
        how='left'
    ).fillna(0).round(2)

    resumen_final.rename(columns={
        'TOTAL_BIO_SEMANA':  'HORAS HECHAS (BIOMÉTRICO)',
        'TOTAL_RECARGO_SEM': 'RECARGOS HECHOS (BIOMÉTRICO)'
    }, inplace=True)

    resumen_final['INTERVALO_FECHAS'] = resumen_final.apply(get_semana_rango_es, axis=1)
    cols = list(resumen_final.columns)
    cols.insert(2, cols.pop(cols.index('INTERVALO_FECHAS')))
    resumen_final = resumen_final[cols]

    progress.progress(100, text="¡Procesamiento completo!")

    return resumen_final, df_detalle_final, df_consolidado


def generar_excel(resumen_final, df_detalle_final, df_consolidado):
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        resumen_final.to_excel(writer,       sheet_name='Resumen_Semanal',      index=False)
        df_detalle_final.to_excel(writer,    sheet_name='Detalle_Cruce_Diario', index=False)
        df_consolidado.to_excel(writer,      sheet_name='Archivo_crudo_limpio', index=False)
    buffer.seek(0)
    return buffer


# ── UI principal ───────────────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("📄 Archivo de Horarios")
    archivo_horarios = st.file_uploader(
        "Sube el archivo de horarios docentes (.xlsx)",
        type=["xlsx"],
        key="horarios"
    )
    if archivo_horarios:
        st.success(f"✅ Cargado: {archivo_horarios.name}")

with col2:
    st.subheader("🖐 Archivo Biométrico")
    archivo_biometrico = st.file_uploader(
        "Sube el archivo biométrico (.xlsx)",
        type=["xlsx"],
        key="biometrico"
    )
    if archivo_biometrico:
        st.success(f"✅ Cargado: {archivo_biometrico.name}")

st.divider()

if archivo_horarios and archivo_biometrico:
    if st.button("🚀 Procesar archivos", type="primary", use_container_width=True):
        try:
            with st.spinner("Procesando..."):
                resumen_final, df_detalle_final, df_consolidado = procesar(
                    archivo_horarios, archivo_biometrico
                )

            st.session_state["resumen_final"]    = resumen_final
            st.session_state["df_detalle_final"] = df_detalle_final
            st.session_state["df_consolidado"]   = df_consolidado
            st.success("✅ Procesamiento exitoso. Revisa las pestañas y descarga el reporte.")

        except Exception as e:
            st.error(f"❌ Error durante el procesamiento: {e}")
            st.exception(e)
else:
    st.info("⬆️ Sube ambos archivos para habilitar el procesamiento.")


    excel_buffer = generar_excel(resumen_final, df_detalle_final, df_consolidado)
    st.download_button(
        label="⬇️ Descargar REPORTE_DOCENTES.xlsx",
        data=excel_buffer,
        file_name="REPORTE_DOCENTES.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary",
        use_container_width=True,
    )
