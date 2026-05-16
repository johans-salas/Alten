# ══════════════════════════════════════════════════════════════════════════════
# Prueba Alten: Segunda Parte - Python + GCP + BQ
# Creado por: Johans Enrique Salas Rodríguez
#
# Descripción:
#   - APIClient: descarga datos meteorológicos históricos (Open-Meteo)
#   - BigQueryLoader: sube los registros a una tabla de BigQuery
# ══════════════════════════════════════════════════════════════════════════════


# ──────────────────────────────────────────────────────────────────────────────
# LIBRERÍAS
# ──────────────────────────────────────────────────────────────────────────────

import json
import logging
from datetime import datetime, timedelta
from typing import Optional
import requests
from google.cloud import bigquery
from google.oauth2 import service_account


# ──────────────────────────────────────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("etl_pipeline.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("ETLPipeline")


# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN GENERAL
# ──────────────────────────────────────────────────────────────────────────────

BQ_PROJECT_ID  = "prueba-alten-js"     # Project ID de Google Cloud
BQ_DATASET_ID  = "SANDBOX_weather"     # Dataset dentro del proyecto
BQ_TABLE_ID    = "raw_weather"         # Tabla cruda dentro del sandbox
BQ_CREDENTIALS = "credentials.json"    # Ruta al JSON de la cuenta de servicio

API_BASE_URL   = "https://api.open-meteo.com/v1/forecast"

# Configuración de las ubicaciones y variables a consultar
LOCATIONS = [
    {"lat": 40.4168, "lon": -3.7038, "city": "Madrid"},
    {"lat": 41.3851, "lon":  2.1734, "city": "Barcelona"},
    {"lat": 37.3891, "lon": -5.9845, "city": "Sevilla"},
    {"lat": 39.4699, "lon": -0.3763, "city": "Valencia"},
    {"lat": 43.2630, "lon": -2.9350, "city": "Bilbao"},
]

# Varialbles a solicitar a la API para cada hora
HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "precipitation",
    "wind_speed_10m",
    "wind_direction_10m",
    "weather_code",
]

DAYS_BACK = 1  # Número de días hacia atrás para obtener datos históricos


# ──────────────────────────────────────────────────────────────────────────────
# CLASE 1: APIClient
# ──────────────────────────────────────────────────────────────────────────────

class APIClient:
    """
    Descarga datos meteorológicos históricos de Open-Meteo (sin API key).

    Uso:
        client    = APIClient()
        registros = client.fetch_all()
    """

    def __init__(self, base_url: str = API_BASE_URL, timeout: int = 30):
        self.base_url = base_url
        self.timeout  = timeout
        self.session  = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        logger.info("APIClient inicializado. URL: %s", self.base_url)

    def _build_params(self, location: dict) -> dict:
        end_date   = datetime.utcnow().date()
        start_date = end_date - timedelta(days=DAYS_BACK)
        return {
            "latitude":        location["lat"],
            "longitude":       location["lon"],
            "hourly":          ",".join(HOURLY_VARIABLES),
            "start_date":      start_date.isoformat(),
            "end_date":        end_date.isoformat(),
            "timezone":        "Europe/Madrid",
            "wind_speed_unit": "kmh",
        }
    
    def _fetch_location(self, location: dict) -> list[dict]:
        """Descarga y normaliza los datos de una ciudad."""
        logger.info("  Descargando: %s ...", location["city"])
        try:
            resp = self.session.get(
                self.base_url,
                params=self._build_params(location),
                timeout=self.timeout,
            )
            resp.raise_for_status()
        except requests.exceptions.Timeout:
            logger.error("Timeout para %s.", location["city"])
            return []
        except requests.exceptions.HTTPError as exc:
            logger.error("HTTP error para %s: %s", location["city"], exc)
            return []
        except requests.exceptions.RequestException as exc:
            logger.error("Error de red para %s: %s", location["city"], exc)
            return []

        hourly = resp.json().get("hourly", {})
        times  = hourly.get("time", [])
        if not times:
            logger.warning("Sin datos para %s.", location["city"])
            return []

        registros = []
        for i, ts in enumerate(times):
            registros.append({
                "ciudad":               location["city"],
                "latitud":              location["lat"],
                "longitud":             location["lon"],
                "timestamp_hora":       ts,          # "2026-05-16T15:00"
                "fecha":                ts[:10],     # "2026-05-16"
                "hora":                 int(ts[11:13]),
                "temperatura_c":        self._val(hourly, "temperature_2m", i),
                "humedad_pct":          self._val(hourly, "relative_humidity_2m", i),
                "temp_aparente_c":      self._val(hourly, "apparent_temperature", i),
                "precipitacion_mm":     self._val(hourly, "precipitation", i),
                "velocidad_viento_kmh": self._val(hourly, "wind_speed_10m", i),
                "dir_viento_grados":    self._val(hourly, "wind_direction_10m", i),
                "codigo_clima":         self._val(hourly, "weather_code", i),
                "cargado_en_utc":       datetime.utcnow().isoformat(timespec="seconds"),
            })

        logger.info("  -> %d registros para %s.", len(registros), location["city"])
        return registros

    @staticmethod
    def _val(hourly: dict, var: str, idx: int):
        vals = hourly.get(var, [])
        return vals[idx] if idx < len(vals) else None

    def fetch_all(self, locations: Optional[list] = None) -> list[dict]:
        """Descarga datos de todas las ciudades y devuelve la lista completa."""
        locs  = locations or LOCATIONS
        todos = []
        logger.info("Iniciando descarga para %d ciudades ...", len(locs))
        for loc in locs:
            todos.extend(self._fetch_location(loc))
        logger.info("Total registros descargados: %d", len(todos))
        return todos
    

# ──────────────────────────────────────────────────────────────────────────────
# CLASE 2: BigQueryLoader
# ──────────────────────────────────────────────────────────────────────────────

class BigQueryLoader:
    """
    Carga registros en el dataset SANDBOX_ de Google BigQuery.
    Crea el dataset y la tabla automáticamente si no existen.

    Uso:
        loader  = BigQueryLoader()
        n_filas = loader.load(registros)
    """

    # Esquema de la tabla raw en BigQuery
    SCHEMA = [
        bigquery.SchemaField("ciudad",               "STRING",  mode="REQUIRED"),
        bigquery.SchemaField("latitud",              "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("longitud",             "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("timestamp_hora",       "STRING",  mode="REQUIRED"),
        bigquery.SchemaField("fecha",                "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("hora",                 "INT64",   mode="NULLABLE"),
        bigquery.SchemaField("temperatura_c",        "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("humedad_pct",          "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("temp_aparente_c",      "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("precipitacion_mm",     "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("velocidad_viento_kmh", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("dir_viento_grados",    "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("codigo_clima",         "INT64",   mode="NULLABLE"),
        bigquery.SchemaField("cargado_en_utc",       "STRING",  mode="NULLABLE"),
    ]

    def __init__(
        self,
        project_id:  str = BQ_PROJECT_ID,
        dataset_id:  str = BQ_DATASET_ID,
        table_id:    str = BQ_TABLE_ID,
        credentials: str = BQ_CREDENTIALS,
    ):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id   = table_id
        self.table_ref  = f"{project_id}.{dataset_id}.{table_id}"

        creds = service_account.Credentials.from_service_account_file(
            credentials,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self.client = bigquery.Client(project=project_id, credentials=creds)
        logger.info("BigQueryLoader listo. Destino: %s", self.table_ref)

    def _ensure_dataset(self):
        """Crea el dataset SANDBOX_ si no existe."""
        ref          = bigquery.Dataset(f"{self.project_id}.{self.dataset_id}")
        ref.location = "EU"
        try:
            self.client.get_dataset(ref)
            logger.info("Dataset '%s' ya existe.", self.dataset_id)
        except Exception:
            self.client.create_dataset(ref, exists_ok=True)
            logger.info("Dataset '%s' creado.", self.dataset_id)

    def _ensure_table(self):
        """Crea la tabla raw con el esquema definido si no existe."""
        table = bigquery.Table(self.table_ref, schema=self.SCHEMA)
        try:
            self.client.get_table(self.table_ref)
            logger.info("Tabla '%s' ya existe.", self.table_ref)
        except Exception:
            self.client.create_table(table)
            logger.info("Tabla '%s' creada.", self.table_ref)

    def load(self, registros: list[dict], modo: str = "WRITE_APPEND") -> int:
        """
        Inserta registros en BigQuery.

        Args:
            registros : lista de dicts a insertar.
            modo      : "WRITE_APPEND"   → añade a datos existentes (por defecto).
                        "WRITE_TRUNCATE" → borra la tabla antes de insertar.
        Returns:
            Número de filas insertadas.
        """
        if not registros:
            logger.warning("Lista vacía. Nada que insertar.")
            return 0

        self._ensure_dataset()
        self._ensure_table()

        job_config = bigquery.LoadJobConfig(
            schema=self.SCHEMA,
            write_disposition=modo,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        logger.info("Subiendo %d registros ...", len(registros))
        job = self.client.load_table_from_json(
            registros, self.table_ref, job_config=job_config
        )
        job.result()  # espera a que el job termine

        n_filas = self.client.get_table(self.table_ref).num_rows
        logger.info("Carga completada. Filas totales en tabla: %d", n_filas)
        return len(registros)


# ──────────────────────────────────────────────────────────────────────────────
# PUNTO DE ENTRADA
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("INICIO ETL PIPELINE")
    logger.info("=" * 60)

    # 1 — Descargar datos de la API
    client    = APIClient()
    registros = client.fetch_all()

    if not registros:
        logger.error("No se obtuvieron registros. Abortando.")
        raise SystemExit(1)

    # 2 — Guardar copia local JSON (para adjuntar al repositorio)
    output_file = "registros_descargados.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(registros, f, indent=2, ensure_ascii=False)
    logger.info("Copia local guardada en '%s'.", output_file)

    # 3 — Subir a BigQuery  →  SANDBOX_weather.raw_weather
    loader  = BigQueryLoader()
    subidos = loader.load(registros, modo="WRITE_APPEND")

    logger.info("=" * 60)
    logger.info("ETL FINALIZADO — %d registros subidos a %s.%s",
                subidos, BQ_DATASET_ID, BQ_TABLE_ID)
    logger.info("=" * 60)