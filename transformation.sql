-- ══════════════════════════════════════════════════════════════════════════════
-- Prueba Alten: Segunda Parte - Python + GCP + BQ
-- Creado por: Johans Enrique Salas Rodríguez
--
-- Descripción:
-- Archivo .sql que lee los datos crudos del día actual desde la tabla
-- SANDBOX_weather.raw_weather y elimina duplicados (misma ciudad + misma 
-- fecha/hora), añade una columna con la fecha/hora de ejecución de la consulta
-- y sube los registros a la tabla INTEGRATION.integration_prueba_tecnica.
-- ══════════════════════════════════════════════════════════════════════════════

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
            PARTITION BY ciudad, timestamp_hora   -- clave de unicidad
            ORDER BY cargado_en_utc DESC          -- en caso de duplicado, queda el registro más reciente
        ) AS rn
    FROM
        `prueba-alten-js.SANDBOX_weather.raw_weather`
    WHERE
        -- Solo procesamos los datos del día de hoy (idempotente por día)
        fecha = CAST(CURRENT_DATE('Europe/Madrid') AS STRING)
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
        -- Nueva columna: cuándo se ejecutó la transformación
        CURRENT_TIMESTAMP() AS transformado_en,
        -- Columna extra: clasificación simple de la temperatura
        CASE
            WHEN temperatura_c < 0                   THEN 'Helada'
            WHEN temperatura_c BETWEEN 0  AND 10     THEN 'Fria'
            WHEN temperatura_c BETWEEN 10 AND 20     THEN 'Templada'
            WHEN temperatura_c BETWEEN 20 AND 30     THEN 'Calida'
            ELSE                                          'Muy calida'
        END                                          AS categoria_temperatura
    FROM datos_deduplicados
    WHERE rn = 1   -- Descarta los duplicados
)

-- MERGE: garantiza idempotencia en la tabla destino
-- Así al ejecutar el script N veces produce exactamente el mismo resultado

MERGE `prueba-alten-jsN.integration_prueba_tecnica` AS destino
USING datos_transformados AS origen
ON  destino.ciudad         = origen.ciudad
AND destino.timestamp_hora = origen.timestamp_hora

-- Fila ya existe: actualizamos todos los campos (por si cambió algún valor)
WHEN MATCHED THEN
    UPDATE SET
        destino.latitud              = origen.latitud,
        destino.longitud             = origen.longitud,
        destino.fecha                = origen.fecha,
        destino.hora                 = origen.hora,
        destino.temperatura_c        = origen.temperatura_c,
        destino.humedad_pct          = origen.humedad_pct,
        destino.temp_aparente_c      = origen.temp_aparente_c,
        destino.precipitacion_mm     = origen.precipitacion_mm,
        destino.velocidad_viento_kmh = origen.velocidad_viento_kmh,
        destino.dir_viento_grados    = origen.dir_viento_grados,
        destino.codigo_clima         = origen.codigo_clima,
        destino.cargado_en_utc       = origen.cargado_en_utc,
        destino.transformado_en      = origen.transformado_en,
        destino.categoria_temperatura= origen.categoria_temperatura

-- Fila nueva: insertamos
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