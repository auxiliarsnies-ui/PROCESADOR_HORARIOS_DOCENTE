import streamlit as st
import holidays
import pandas as pd
import numpy as np
import re
import io
from datetime import datetime, timedelta

# ── Constantes ────────────────────────────────────────────────────────────────
YEAR = 2026
BASE_FECHA = "2026-01-01 "

MESES_ES = {
    1: "ene", 2: "feb", 3: "mar", 4: "abr", 5: "may", 6: "jun",
    7: "jul", 8: "ago", 9: "sep", 10: "oct", 11: "nov", 12: "dic"
}

DIAS_MAP = {
    "Monday": "LU", "Tuesday": "MA", "Wednesday": "MI",
    "Thursday": "JU", "Friday": "VI", "Saturday": "SA", "Sunday": "DO"
}

FESTIVOS_CO = holidays.Colombia(years=YEAR)

# ── Helpers ───────────────────────────────────────────────────────────────────
def get_semana_rango_es(row):
    semana_ajustada = int(row["NUM_SEMANA"]) - 1
    lunes = datetime.strptime(f'{YEAR}-W{semana_ajustada}-1', "%Y-W%W-%w")
    domingo = lunes + timedelta(days=6)
    return f"{lunes.day:02d} {MESES_ES[lunes.month]} - {domingo.day:02d} {MESES_ES[domingo.month]}"

def limpiar_horarios(texto):
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
    h_ini = pd.to_datetime(BASE_FECHA + df[col_ini].astype(str), errors='coerce')
    h_fin = pd.to_datetime(BASE_FECHA + df[col_fin].astype(str), errors='coerce')
    diff = (h_fin - h_ini).dt.total_seconds() / 3600
    return np.where(diff < 0, diff + 24, diff)

def calcular_recargo(df, col_ini, col_fin, inicio="19:00", fin="22:00"):
    h_ini = pd.to_datetime(BASE_FECHA + df[col_ini].astype(str), errors='coerce')
    h_fin = pd.to_datetime(BASE_FECHA + df[col_fin].astype(str), errors='coerce')
    p_inicio = pd.to_datetime(BASE_FECHA + inicio)
    p_fin    = pd.to_datetime(BASE_FECHA + fin)
    diferencia = (h_fin.clip(upper=p_fin) - h_ini.clip(lower=p_inicio)).dt.total_seconds() / 3600
    return diferencia.clip(lower=0).fillna(0).round(2)

def calcular_recargos_reales(row):
    if str(row['ENTRADA_BIO']).upper() == 'SIN MARCA' or pd.isna(row['ENTRADA_BIO']):
        return 0.0
    if row['recargo_proyectado'] <= 0:
        return 0.0
    try:
        real_in  = pd.to_datetime(BASE_FECHA + str(row['ENTRADA_BIO']), errors='coerce')
        real_out = pd.to_datetime(BASE_FECHA + str(row['SALIDA_BIO']),  errors='coerce')
        proj_out = pd.to_datetime(BASE_FECHA + str(row['FIN_CLASE']),   errors='coerce')
        if pd.isna(real_in) or pd.isna(real_out) or pd.isna(proj_out):
            return 0.0
        inicio_recargo = proj_out - pd.Timedelta(hours=row['recargo_proyectado'])
        inicio_v = max(real_in, inicio_recargo)
        fin_v    = min(real_out, proj_out)
        if fin_v > inicio_v:
            return round((fin_v - inicio_v).total_seconds() / 3600, 2)
    except Exception as e:
        return 0.0
    return 0.0

def procesar(file_carga, file_bio, file_aus, progress):

    # ── 1. Carga y limpieza inicial
    progress.progress(5, "Leyendo archivo de carga...")
    df = pd.read_excel(file_carga)
    df["HORAS"] = df["HORAS"].str.replace("NO TIENE", "0")
    df["MATERIA_INI"] = pd.to_datetime(df["MATERIA_INI"], dayfirst=True)
    df["MATERIA_FIN"] = pd.to_datetime(df["MATERIA_FIN"], dayfirst=True)
    df1 = df.drop(columns=['MATERIA_ACTIVIDAD', 'SEDE', 'GRUPO', 'COD_MATERIA', 'CENTRO_COSTO', 'NPLAN', 'DEDICACIÓN'])

    # ── 2. Explotar horarios por fila
    progress.progress(15, "Procesando horarios...")
    df1["horarios_lista"] = df1["HORAS"].apply(limpiar_horarios)
    df1 = df1.explode("horarios_lista").reset_index(drop=True)
    df_final = pd.concat([df1.drop(columns=["horarios_lista"]),
                          pd.json_normalize(df1["horarios_lista"])], axis=1)

    # ── 3. Separar clases normales vs complementarias
    progress.progress(25, "Separando clases y complementarias...")
    SIN_HORARIO = [0, "0", None, ""]
    df_clases = df_final[~df_final["HORAS"].isin(SIN_HORARIO)]
    df_compl  = df_final[ df_final["HORAS"].isin(SIN_HORARIO)]

    COLS_GRUPO = ["CODIGO", "DOCUMENTO", "NOMBRE", "MATERIA_INI", "MATERIA_FIN"]
    df_resumen_compl = (
        df_compl.groupby(COLS_GRUPO)
                .agg(TOTAL_HORAS=("TOTAL_HORAS", "sum"))
                .reset_index()
                .assign(dia="COMPL", HORAS="TOTAL SEMANAL")
    )

    df_final = pd.concat([df_clases, df_resumen_compl], ignore_index=True)
    df_final.sort_values("NOMBRE", inplace=True)

    # ── 4. Expandir clases a fechas diarias
    progress.progress(35, "Expandiendo calendario semestral...")
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

    df_clases_diarias = pd.concat(filas_expandidas, ignore_index=True)
    df_calendario = pd.concat([df_clases_diarias, df_resumen_semanal], ignore_index=True)
    df_calendario.sort_values(["NOMBRE", "fecha"], inplace=True)
    df_calendario = df_calendario.drop(columns=["HORAS"])

    # ── 5. Quitar festivos y Semana Santa
    progress.progress(45, "Quitando festivos y Semana Santa...")
    SEMANA_SANTA = ("2026-03-29", "2026-04-05")
    df_calendario = df_calendario[
        (df_calendario["fecha"] < SEMANA_SANTA[0]) |
        (df_calendario["fecha"] > SEMANA_SANTA[1]) |
        (df_calendario["dia"] == "COMPL")
    ]
    df_calendario = df_calendario[~df_calendario["fecha"].dt.date.isin(FESTIVOS_CO.keys())]

    # ── 5.1. Duración y recargo proyectado
    df_calendario['duracion_clase'] = (
        pd.to_datetime(df_calendario['hora_fin'],    format='%H:%M') -
        pd.to_datetime(df_calendario['hora_inicio'], format='%H:%M')
    ).dt.total_seconds() / 3600
    df_calendario['recargo_proyectado'] = calcular_recargo(df_calendario, 'hora_inicio', 'hora_fin')

    # ── 6. Agrupar por día
    progress.progress(55, "Agrupando por día...")
    df_agrupado = (
        df_calendario[df_calendario["dia"] != "COMPL"]
        .groupby(['DOCUMENTO', 'fecha']).agg(
            Entrada_Real       = ('hora_inicio',        'min'),
            Salida_Real        = ('hora_fin',           'max'),
            horas_laborales    = ('duracion_clase',     'sum'),
            recargo_proyectado = ('recargo_proyectado', 'sum')
        )
        .reset_index()
    )

    # ── 7. Unir complementarias y construir consolidado
    progress.progress(60, "Consolidando datos...")
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

    # ── 8. Llave y orden final
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

    # ── Biométrico
    progress.progress(65, "Leyendo biométrico...")
    biometrico = pd.read_excel(file_bio,
                               usecols=["fecha", "Documento", "hora_entrada", "hora_salida", "horas"],
                               skiprows=1)
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

    # ── Cruce con consolidado
    progress.progress(70, "Cruzando con biométrico...")
    df_detalle_final = pd.merge(
        df_consolidado,
        biometrico_unificado[['LLAVE', 'HORA_ENTRADA', 'HORA_SALIDA', 'HORAS']],
        left_on='llave', right_on='LLAVE',
        how='left'
    ).drop(columns=['LLAVE'])
    df_detalle_final.rename(columns={
        'Entrada_Real':    'INI_CLASE',
        'Salida_Real':     'FIN_CLASE',
        'horas_laborales': 'HORAS_CLASE',
        'HORA_ENTRADA':    'ENTRADA_BIO',
        'HORA_SALIDA':     'SALIDA_BIO',
        'HORAS':           'TOTAL_BIO_DIA'
    }, inplace=True)
    cols = ['llave'] + [c for c in df_detalle_final.columns if c != 'llave']
    df_detalle_final = df_detalle_final[cols]

    # ── Recargos nocturnos
    progress.progress(78, "Calculando recargos nocturnos...")
    df_detalle_final['TOTAL_HORAS_RECARGO'] = 0.0
    mask_recargo = df_detalle_final['recargo_proyectado'] > 0
    df_detalle_final.loc[mask_recargo, 'TOTAL_HORAS_RECARGO'] = (
        df_detalle_final[mask_recargo].apply(calcular_recargos_reales, axis=1)
    )

    # ── Semana
    df_detalle_final['NUM_SEMANA'] = pd.to_datetime(df_detalle_final['fecha']).dt.isocalendar().week

    # ── Ausentismos (opcional)
    progress.progress(83, "Procesando ausentismos...")
    if file_aus is not None:
        ausentismos = pd.read_excel(file_aus, usecols=["fecha_ina", "cod_emp"], skiprows=1)
        ausentismos.columns = ausentismos.columns.str.upper().str.strip()
        ausentismos['FECHA_INA'] = pd.to_datetime(ausentismos['FECHA_INA'], dayfirst=True, errors='coerce')
        llave_aus = ausentismos['FECHA_INA'].dt.strftime('%d/%m/%Y') + '-' + ausentismos['COD_EMP'].astype(str)
        ausentismos.insert(0, 'llave', llave_aus)
        mascara_ausencia     = df_detalle_final['llave'].isin(ausentismos['llave'])
        mascara_sin_marcacion = (df_detalle_final['TOTAL_BIO_DIA'] == 0) | (df_detalle_final['TOTAL_BIO_DIA'].isna())
        condicion_final = mascara_ausencia & mascara_sin_marcacion
        df_detalle_final.loc[condicion_final, 'TOTAL_BIO_DIA'] = df_detalle_final.loc[condicion_final, 'HORAS_CLASE']
        df_detalle_final['AUSENTISMO'] = np.where(df_detalle_final['llave'].isin(ausentismos['llave']), 'SI', 'NO')
    else:
        df_detalle_final['AUSENTISMO'] = 'NO'

    # ── Biométrico semanal
    progress.progress(87, "Calculando resumen semanal...")
    biometrico_semanal = (
        df_detalle_final
        .groupby(['DOCUMENTO', 'NUM_SEMANA'])
        .agg(
            TOTAL_BIO_SEMANA  =('TOTAL_BIO_DIA',      'sum'),
            TOTAL_RECARGO_SEM =('TOTAL_HORAS_RECARGO', 'sum')
        )
        .reset_index()
    )

    # ── Resumen semanal / balance final
    progress.progress(90, "Construyendo balance final...")
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
        .agg(HORAS_VALOR=('HORAS_VALOR', 'sum'), RECARGOS_VALOR=('RECARGOS_VALOR', 'sum'))
        .reset_index()
        .rename(columns={'HORAS_VALOR': 'HORAS QUE DEBIA HACER', 'RECARGOS_VALOR': 'RECARGOS QUE DEBIA HACER'})
    )
    resumen_final = pd.merge(resumen_final, biometrico_semanal, on=['DOCUMENTO', 'NUM_SEMANA'], how='left').fillna(0).round(2)
    resumen_final.rename(columns={
        'TOTAL_BIO_SEMANA':  'HORAS HECHAS (BIOMÉTRICO)',
        'TOTAL_RECARGO_SEM': 'RECARGOS HECHOS (BIOMÉTRICO)'
    }, inplace=True)
    resumen_final['INTERVALO_FECHAS'] = resumen_final.apply(get_semana_rango_es, axis=1)
    cols = list(resumen_final.columns)
    cols.insert(2, cols.pop(cols.index('INTERVALO_FECHAS')))
    resumen_final = resumen_final[cols]

    # ── Traer columnas al detalle final Y resumen final semanal
    progress.progress(93, "Enriqueciendo detalle final...")
    df_unicos = df[['DOCUMENTO', 'SEDE', 'NOMBRE', 'CENTRO_COSTO', 'DEDICACIÓN']].drop_duplicates(subset=['DOCUMENTO'])
    df_detalle_final = pd.merge(df_detalle_final, df_unicos, on='DOCUMENTO', how='left')
    columnas_ordenadas = [
        'llave', 'DOCUMENTO', 'NOMBRE', 'fecha', 'NUM_SEMANA', 'SEDE', 'CENTRO_COSTO', 'DEDICACIÓN',
        'INI_CLASE', 'FIN_CLASE', 'HORAS_CLASE', 'ENTRADA_BIO',
        'SALIDA_BIO', 'TOTAL_BIO_DIA', 'recargo_proyectado', 'TOTAL_HORAS_RECARGO', 'AUSENTISMO'
    ]
    df_detalle_final = df_detalle_final[columnas_ordenadas]

    resumen_final = pd.merge(resumen_final, df_unicos, on='DOCUMENTO', how='left')
    columnas_ordenadas = ['DOCUMENTO','SEDE','CENTRO_COSTO','DEDICACIÓN','NUM_SEMANA','INTERVALO_FECHAS','HORAS QUE DEBIA HACER',
                      'RECARGOS QUE DEBIA HACER','HORAS HECHAS (BIOMÉTRICO)','RECARGOS HECHOS (BIOMÉTRICO)']
    resumen_final = resumen_final[columnas_ordenadas]


    # ── Alivio de 15 minutos
    progress.progress(96, "Aplicando alivio de 15 minutos...")
    hora_completa   = np.ceil(df_detalle_final['TOTAL_HORAS_RECARGO'])
    tiempo_faltante = hora_completa - df_detalle_final['TOTAL_HORAS_RECARGO']
    condicion_alivio = (tiempo_faltante > 0) & (tiempo_faltante <= 0.25)
    df_detalle_final.loc[condicion_alivio, 'TOTAL_HORAS_RECARGO'] = hora_completa[condicion_alivio]

    # ── Exportar a buffer
    progress.progress(98, "Generando archivo Excel...")
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        resumen_final.to_excel(writer,      sheet_name='Detalle semanal',       index=False)
        df_detalle_final.to_excel(writer,   sheet_name='Detalle_Cruce_Diario',  index=False)
        df_consolidado.to_excel(writer,     sheet_name='Archivo_crudo_limpio',  index=False)
    buffer.seek(0)

    progress.progress(100, "¡Listo!")
    return buffer


# ── UI Streamlit ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="Reporte Docentes", page_icon="📋", layout="centered")
st.title("📋 Reporte Docentes")
st.markdown("Carga los archivos para generar el reporte consolidado.")

col1, col2 = st.columns(2)
with col1:
    file_carga = st.file_uploader("Archivo de Carga *", type=["xlsx"])
    file_bio   = st.file_uploader("Archivo Biométrico *", type=["xlsx"])
with col2:
    file_aus   = st.file_uploader("Ausentismos (opcional)", type=["xlsx"])

st.divider()

archivos_listos = file_carga is not None and file_bio is not None

if st.button("▶ Procesar", disabled=not archivos_listos, use_container_width=True, type="primary"):
    progress = st.progress(0, "Iniciando...")
    try:
        buffer = procesar(file_carga, file_bio, file_aus, progress)
        st.session_state['resultado'] = buffer
        st.success("Procesamiento completado.")
    except Exception as e:
        st.error(f"Error durante el procesamiento: {e}")

if 'resultado' in st.session_state:
    st.download_button(
        label="⬇ Descargar REPORTE_DOCENTES.xlsx",
        data=st.session_state['resultado'],
        file_name="REPORTE_DOCENTES.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        type="primary"
    )
