# ══════════════════════════════════════════════════════════════════════════════
# Prueba Alten: Tercera Parte - Airflow
# Creado por: Johans Enrique Salas Rodríguez
#
# ── ¿Qué es un Hook? ¿En qué se diferencia de una conexión? (Punto 5) ────────
# CONEXIÓN (Connection):
#   Es la configuración almacenada en la base de datos de Airflow que contiene
#   los datos necesarios para conectarse a un sistema externo: host, puerto,
#   usuario, contraseña, esquema, etc.
#   Se gestiona desde la UI de Airflow (Admin → Connections) o con variables
#   de entorno. Es simplemente un objeto de configuración, no hace nada por
#   sí sola.
#   Ejemplo: una conexión llamada "my_postgres" con host="localhost", port=5432.
#
# HOOK:
#   Es una clase de Python que actúa como interfaz de alto nivel para
#   interactuar con un sistema externo (base de datos, API, S3, BigQuery...).
#   El Hook usa internamente una Conexión de Airflow para obtener las
#   credenciales y parámetros, pero expone métodos listos para usar
#   (get_records, run, upload, download, etc.).
#   Ejemplo: PostgresHook("my_postgres").get_records("SELECT * FROM tabla")
#
# DIFERENCIA ENTRE HOOK Y UNA CONEXIÓN:
#   - La Conexión es el "qué" (configuración, credenciales, dirección).
#   - El Hook es el "cómo" (lógica de conexión, métodos de interacción).
#   Un Hook sin Conexión no sabe a dónde conectarse.
#   Una Conexión sin Hook es solo un diccionario de datos inerte.
#   Los Operators de Airflow usan Hooks internamente para no mezclar
#   lógica de negocio con lógica de conectividad.
# ══════════════════════════════════════════════════════════════════════════════


# ──────────────────────────────────────────────────────────────────────────────
# LIBRERÍAS
# ──────────────────────────────────────────────────────────────────────────────

from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.decorators import apply_defaults

class TimeDiffOperator(BaseOperator):
    """
    Operador personalizado que recibe una fecha (diff_date) y calcula
    la diferencia con la fecha y hora actual en el momento de ejecución.

    Args:
        diff_date (datetime): fecha con la que comparar el momento actual.
    """
    # apply_defaults inyecta automáticamente los argumentos por defecto del DAG
    @apply_defaults
    def __init__(self, diff_date: datetime, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.diff_date = diff_date

    def execute(self, context):
        """
        Método ejecutado por Airflow cuando llega el turno de esta tarea.
        context contiene metadatos del DAG run (fechas, task_instance, etc.)
        """
        ahora      = datetime.now(timezone.utc)  # Obtener la fecha y hora actual en UTC    
        diferencia = ahora - self.diff_date

        dias    = diferencia.days
        horas   = diferencia.seconds // 3600
        minutos = (diferencia.seconds % 3600) // 60
        segundos= diferencia.seconds % 60

        self.log.info("=" * 50)
        self.log.info("TimeDiffOperator — resultado:")
        self.log.info("  Fecha de referencia : %s", self.diff_date.isoformat())
        self.log.info("  Fecha actual (UTC)  : %s", ahora.isoformat())
        self.log.info(
            "  Diferencia          : %d días, %02d:%02d:%02d (hh:mm:ss)",
            dias, horas, minutos, segundos
        )
        self.log.info("=" * 50)

        # Devolver el resultado permite capturarlo si otro task lo necesita
        return {
            "diff_date":  self.diff_date.isoformat(),
            "ahora_utc":  ahora.isoformat(),
            "dias":       dias,
            "horas":      horas,
            "minutos":    minutos,
            "segundos":   segundos,
        }