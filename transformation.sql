/*
# ══════════════════════════════════════════════════════════════════════════════
# Prueba Alten: Segunda Parte - Python + GCP + BQ
# Creado por: Johans Enrique Salas Rodríguez
#
# Descripción:
	1. Lee los datos crudos del día actual desde SANDBOX_weather.raw_weather
	2. Elimina duplicados (misma ciudad + misma hora)
	3. Añade una columna extra con la fecha/hora de ejecución de la transformación
	   y otra con una clasificación simple del nivel de la temperatura
	4. Actualiza/Inserta el resultado en INTEGRATION.integration_prueba_tecnica
# ══════════════════════════════════════════════════════════════════════════════
*/
MERGE `prueba-alten-js.INTEGRATION.integration_prueba_tecnica` AS destino
USING (
    WITH datos_deduplicados AS (
        SELECT
            ciudad,
            latitud,
            longitud,
            timestamp_hora,
            fecha,
            hora,
            temperatura_c,
            humedad_pct,
            temp_aparente_c,
            precipitacion_mm,
            velocidad_viento_kmh,
            dir_viento_grados,
            codigo_clima,
            cargado_en_utc,
            ROW_NUMBER() OVER (
                PARTITION BY ciudad, timestamp_hora
                ORDER BY cargado_en_utc DESC
            ) AS rn
        FROM `prueba-alten-js.SANDBOX_weather.raw_weather`
        WHERE fecha = CURRENT_DATE('Europe/Madrid')
    ),
    datos_transformados AS (
        SELECT
            ciudad,
            latitud,
            longitud,
            timestamp_hora,
            fecha,
            hora,
            temperatura_c,
            humedad_pct,
            temp_aparente_c,
            precipitacion_mm,
            velocidad_viento_kmh,
            dir_viento_grados,
            codigo_clima,
            cargado_en_utc,
            CURRENT_TIMESTAMP() AS transformado_en,
            CASE
                WHEN temperatura_c < 0 THEN 'Helada'
                WHEN temperatura_c >= 0  AND temperatura_c < 10 THEN 'Fria'
                WHEN temperatura_c >= 10 AND temperatura_c < 20 THEN 'Templada'
                WHEN temperatura_c >= 20 AND temperatura_c < 30 THEN 'Calida'
                ELSE 'Muy calida'
            END AS categoria_temperatura
        FROM datos_deduplicados
        WHERE rn = 1
    )
    SELECT * FROM datos_transformados
) AS origen
ON destino.ciudad = origen.ciudad
AND destino.timestamp_hora = origen.timestamp_hora
WHEN MATCHED THEN
UPDATE SET
    latitud               = origen.latitud,
    longitud              = origen.longitud,
    fecha                 = origen.fecha,
    hora                  = origen.hora,
    temperatura_c         = origen.temperatura_c,
    humedad_pct           = origen.humedad_pct,
    temp_aparente_c       = origen.temp_aparente_c,
    precipitacion_mm      = origen.precipitacion_mm,
    velocidad_viento_kmh  = origen.velocidad_viento_kmh,
    dir_viento_grados     = origen.dir_viento_grados,
    codigo_clima          = origen.codigo_clima,
    cargado_en_utc        = origen.cargado_en_utc,
    transformado_en       = origen.transformado_en,
    categoria_temperatura = origen.categoria_temperatura
WHEN NOT MATCHED THEN
INSERT (
    ciudad,
    latitud,
    longitud,
    timestamp_hora,
    fecha,
    hora,
    temperatura_c,
    humedad_pct,
    temp_aparente_c,
    precipitacion_mm,
    velocidad_viento_kmh,
    dir_viento_grados,
    codigo_clima,
    cargado_en_utc,
    transformado_en,
    categoria_temperatura
)
VALUES (
    origen.ciudad,
    origen.latitud,
    origen.longitud,
    origen.timestamp_hora,
    origen.fecha,
    origen.hora,
    origen.temperatura_c,
    origen.humedad_pct,
    origen.temp_aparente_c,
    origen.precipitacion_mm,
    origen.velocidad_viento_kmh,
    origen.dir_viento_grados,
    origen.codigo_clima,
    origen.cargado_en_utc,
    origen.transformado_en,
    origen.categoria_temperatura
);
