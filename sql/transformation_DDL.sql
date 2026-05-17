CREATE OR REPLACE TABLE
`prueba-alten-js.INTEGRATION.integration_prueba_tecnica`
AS
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
    WHERE CAST(fecha AS DATE) = CURRENT_DATE('Europe/Madrid')
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
SELECT *
FROM datos_transformados;