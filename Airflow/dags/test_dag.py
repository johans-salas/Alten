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
from airflow.models import BaseOperator, Param 
from airflow.operators.dummy import DummyOperator
from airflow.utils.decorators import apply_defaults

# ──────────────────────────────────────────────────────────────────────────────
# OPERADOR PERSONALIZADO: TimeDiffOperator
# ──────────────────────────────────────────────────────────────────────────────

class TimeDiffOperator(BaseOperator):
    """
    Operador personalizado que recibe una fecha (diff_date) y calcula
    la diferencia con la fecha y hora actual en el momento de ejecución.

    Args:
        diff_date (str | datetime): fecha de referencia.
            Puede ser datetime o string 'YYYY-MM-DD HH:MM:SS' / 'YYYY-MM-DD'.
            Si se pasa mediante Params del DAG, llega siempre como string.
    """
    # apply_defaults inyecta automáticamente los argumentos por defecto del DAG
    @apply_defaults
    def __init__(self, diff_date: datetime, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.diff_date = diff_date # valor por defecto si no hay params en el run

    def execute(self, context):
        """
        Airflow llama a este método al ejecutar la tarea.

        Lee diff_date desde context["params"] — lo que el usuario introdujo
        al hacer "Trigger DAG w/ config" en la UI o por CLI con --conf.
        Si no se pasó ninguno, usa self.diff_date como valor por defecto.
        """
        # Leer valor del param en el tiempo de ejecución
        raw_date = context["params"].get("diff_date", self.diff_date)

        # Convertir a datetime con timezone UTC 
        if isinstance(raw_date, str):
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    diff_date_dt = datetime.strptime(raw_date, fmt).replace(
                        tzinfo=timezone.utc
                    )
                    break
                except ValueError:
                    continue
            else:
                raise ValueError(
                    f"Formato de fecha no reconocido: '{raw_date}'. "
                    "Usa 'YYYY-MM-DD' o 'YYYY-MM-DD HH:MM:SS'."
                )
        elif isinstance(raw_date, datetime):
            diff_date_dt = (
                raw_date if raw_date.tzinfo
                else raw_date.replace(tzinfo=timezone.utc)
            )
        else:
            raise TypeError(
                f"diff_date debe ser str o datetime, recibido: {type(raw_date)}"
            )
        
        # Cálculo de la diferencia con la fecha actual en UTC
        ahora      = datetime.now(timezone.utc)  # Obtener la fecha y hora actual en UTC    
        diferencia = ahora - diff_date_dt

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


# ──────────────────────────────────────────────────────────────────────────────
# ARGUMENTOS POR DEFECTO DEL DAG
# ──────────────────────────────────────────────────────────────────────────────

default_args = {
    'owner':            'airflow',
    'depends_on_past':  False,
    'start_date':       datetime(1900, 1, 1),
    'retries':          1,
    'retry_delay':      timedelta(seconds=5),
}


# ──────────────────────────────────────────────────────────────────────────────
# PARAMS — valores editables desde la UI de Airflow
#
# Cómo cambiarlos en la UI:
#   1. Clic en el DAG "test"
#   2. Botón "Trigger DAG" → "Trigger DAG w/ config"
#   3. Se abre un formulario JSON donde puedes escribir:
#      {
#        "n_tareas": 8,
#        "diff_date": "2000-06-15 12:30:00"
#      }
#
# Cómo cambiarlos por línea de comandos (dentro del contenedor Docker):
#   airflow dags trigger test --conf '{"n_tareas": 4, "diff_date": "1990-01-01"}'
#
# Si no se cambia nada, se usan los valores por defecto.
# ──────────────────────────────────────────────────────────────────────────────

DAG_PARAMS = {
    "n_tareas": Param(
        default=6,
        type="integer",
        minimum=2,
        description=(
            "Número total de tareas dummy a generar. "
            "Mínimo 2 (para que haya al menos 1 par y 1 impar). "
            "Ejemplo: 6 genera task_1 ... task_6."
        ),
    ),
    "diff_date": Param(
        default="1970-01-01 00:00:00",
        type="string",
        description=(
            "Fecha de referencia para TimeDiffOperator. "
            "Formato aceptado: 'YYYY-MM-DD' o 'YYYY-MM-DD HH:MM:SS'. "
            "Ejemplo: '2000-01-01' o '1995-07-20 08:30:00'."
        ),
    ),
}


# ──────────────────────────────────────────────────────────────────────────────
# DEFINICIÓN DEL DAG
# ──────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id='test',
    default_args=default_args,
    schedule_interval='0 3 * * *',   # Cron: minuto=0, hora=3, todos los días
    catchup=False,                   # No ejecuta los runs históricos atrasados
    params=DAG_PARAMS,               # Registro de parámetros editables desde la UI
    tags=['prueba_tecnica', 'parte3'],
    description='DAG de prueba técnica — TimeDiff + tareas dummy encadenadas',
) as dag:
    
    # Tareas DummyOperator de Inicio y Fin
    start = DummyOperator(task_id='start')
    end   = DummyOperator(task_id='end')

    N = DAG_PARAMS["n_tareas"].value   # Lee el valor por defecto (6) o el que el usuario haya pasado al trigger

    tareas = {
        n: DummyOperator(task_id=f"task_{n}")
        for n in range(1, N + 1)
    }
 
    tareas_impares = [tareas[n] for n in range(1, N + 1) if n % 2 != 0]
    tareas_pares   = [tareas[n] for n in range(1, N + 1) if n % 2 == 0]

    # Cada tarea par depende de todas las tareas impares
    for tarea_par in tareas_pares:
        for tarea_impar in tareas_impares:
            tarea_impar >> tarea_par

    # TimeDiffOperator 
        time_diff_task = TimeDiffOperator(
        task_id='time_diff',
        diff_date=DAG_PARAMS["diff_date"].value,
    )

    # Definición del flujo completo del DAG    
    start >> time_diff_task >> tareas_impares
    tareas_pares >> end