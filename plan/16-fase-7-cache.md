# 16 — Fase 7: caché basada en contenido

- **Fecha:** 2026-11-25
- **Estado:** en redacción
- **Paso:** Fase 7 de [03-fases-y-dependencias.md](03-fases-y-dependencias.md)
- **Requisito:** R9 — Caché basada en contenido para evitar trabajo idéntico cuando las entradas no cambian
- **Corte:** C7 — una tarea cuyo comando, entorno, entradas y salidas declaradas son idénticos a una ronda anterior exitosa devuelve el resultado guardado sin ejecutar nada

## 1. Alcance y estado

Las features son las de [01-analisis-de-features.md](01-analisis-de-features.md) §2.8; aquí
solo se sigue su avance.

| Feature | Qué | Estado |
|---------|-----|--------|
| F9.1 | Clave de acción: hash del comando, el entorno, las entradas (ruta→digest), las salidas declaradas y la plataforma | Implementada (Commit 2) |
| F9.2 | Consulta antes de despachar; con acierto la tarea no se ejecuta y sus artefactos quedan disponibles | Implementada (Commit 4) |
| F9.3 | Solo se guardan los éxitos | Implementada (Commit 3) |
| F9.4 | Claves encadenadas: la clave usa los digests de salida de las dependencias | Implementada (Commit 4) |
| F9.5 | Exclusión por tarea (`cache: false`) | Implementada (Commit 4) |
| F9.6 | Dos tareas idénticas en vuelo: la segunda espera a la primera | Fuera de C7 ([17](17-futuras-extensiones.md), D16.9) |

## 2. Decisiones

### D16.1 — La clave de caché es un hash de contenido, no una etiqueta

- **Contexto:** R9 pide evitar trabajo idéntico cuando las entradas no cambian. La especificación de una tarea (F1.1) incluye comando, variables de entorno, entradas (ruta → digest) y salidas declaradas. Dos tareas con la misma especificación deben producir el mismo resultado.
- **Opciones:** (a) una clave compuesta por los campos en bruto (JSON canónico); (b) un hash criptográfico (SHA-256) de la serialización canónica; (c) un identificador asignado por el usuario.
- **Decisión:** (b) SHA-256 de la serialización canónica de `(command, env, inputs, outputs_declared, platform_id, key_format_version)`: JSON con claves ordenadas, sin espacios y en UTF-8, con las rutas de salida ordenadas antes de serializar. Se implementa en `dpes/hashing.py` como `action_key(command, env, inputs, outputs, platform_id, key_format_version=ACTION_KEY_VERSION)`, que devuelve 64 hexadecimales en minúscula, sin prefijo. Es la clave de acción de D02.12.
- **Porqué:** (a) hace las claves largas y frágiles ante cambios irrelevantes de formato; (c) traslada la corrección al usuario y rompe la garantía de «entradas idénticas → mismo trabajo». Un hash fijo de 64 hexadecimales es compacto, determinista y no requiere coordinación central para asignar IDs. Entran solo los campos que deciden el resultado: la plataforma evita servir un binario compilado con otro compilador; el orden de las salidas declaradas no cambia lo que produce el comando, así que ordenarlas evita fallos de caché falsos; el orden de los argumentos del comando sí importa y se conserva (D01.3); la versión del formato permite cambiar la carga más adelante sin colisionar con claves viejas. No lleva prefijo porque la clave no es el nombre de un blob: `hashing.digest` reserva la forma `sha256:` para eso.
- **Consecuencia:** `dpes/hashing.py` gana `action_key`, con las pruebas en `tests/unit/test_hashing.py` (Commit 2). El coordinador le pasa su `platform_id` —imagen y compilador— al consultar la caché (Commit 4).
- **Corrección (2026-11-25, antes de cerrar la fase):** la primera redacción omitía la plataforma y suponía un `hash_spec` que nunca existió. Se alinea aquí con D02.12; no se borra la opción descartada.

### D16.2 — La caché vive en el almacén de resultados (`dpes/store.py`), no en un componente aparte

- **Contexto:** La Fase 6 (D12.1) introdujo `dpes/store.py` con SQLite y la regla «el primero que escribe gana» para guardar `TaskResult` bajo la clave `(job, task, round)`. La caché necesita consultar «¿existe un resultado exitoso para esta especificación?» y, en caso afirmativo, leer sus salidas.
- **Opciones:** (a) tabla `cache` separada con clave `cache_key → result_key`; (b) reutilizar la tabla `results` existente añadiendo una columna `cache_key` y un índice parcial; (c) un almacén de clave-valor distinto (Redis, etcd).
- **Decisión:** (b) Extender `store.py` con una columna `cache_key` en la tabla `results` y un índice parcial `WHERE succeeded = 1`, **no único**. La consulta de caché es `SELECT result FROM results WHERE cache_key = ? AND succeeded = 1 LIMIT 1`.
- **Porqué:** (1) Un solo almacén, un solo esquema, una sola sentencia para «guardar resultado + poblar caché». (2) La regla «el primero que escribe gana» (D12.1) ya resuelve la carrera entre dos trabajadores que terminan la misma ronda: solo uno escribe, el otro leerá ese resultado en su reconciliación. (3) No añade dependencias externas (cumple regla 3 del CLAUDE.md). (4) SQLite con `WAL` soporta lecturas concurrentes mientras se escribe. (5) El índice **no es único** porque dos tareas distintas —distinto `job`, `task` o `round`— pueden hacer el mismo trabajo y ambas deben conservar su fila en `results` (S1, «un resultado por tarea»). Un índice único haría que `INSERT OR IGNORE` descartara la **fila completa** de la segunda tarea, no solo su entrada de caché: se perdería un resultado confirmado y `save` devolvería `None`. Como ambas filas describen el mismo trabajo (misma `action_key`), cualquiera sirve como acierto y `LIMIT 1` elige una. El crecimiento está acotado en la práctica porque un acierto no ejecuta ni escribe: solo un *miss* crea fila.
- **Consecuencia:** `store.py` gana `get_by_cache_key(cache_key: str) -> Optional[TaskResult]` y `save(key, result, cache_key)` escribe la clave en la misma fila cuando `result.succeeded`. El índice parcial deja fuera de la caché lo que no tuvo éxito. El actor `BlobStore` de `coordinator.py`, que hoy reenvía `save_result(key, result)`, expone ambos métodos al coordinador en el Commit 4; `store.py` no cambia más. **Si más adelante se necesita evicción, TTL, métricas de caché o deduplicación en vuelo**, esos requisitos piden un índice canónico con una fila por clave; se añadirá entonces una tabla `cache_entries` (PK `cache_key`, FK a `results`) encima de esta, sin migrar ni tocar `results` ([17-futuras-extensiones.md](17-futuras-extensiones.md), E2).

### D16.3 — Consulta de caché atómica en el coordinador, antes de crear el intento

- **Contexto:** El coordinador decide qué tarea despachar (Fase 2). Antes de lanzar un intento, debe saber si ya existe un resultado en caché para la especificación de esa ronda.
- **Opciones:** (a) el ejecutor consulta la caché al arrancar; (b) el coordinador consulta antes de enviar el `Attempt`; (c) el almacén intercepta el `Attempt` y responde con el resultado si existe.
- **Decisión:** (b) El coordinador, en `_run` y antes de que el intento exista en Ray, calcula la clave de acción de la ronda y llama a `store.cached(cache_key)`. Si hay acierto, asienta la tarea como `SUCCEEDED` con el resultado guardado y **no crea intento**. Un reintento de infraestructura no consulta: su ronda no tiene un éxito confirmado (D16.4), y `Launch.retry` lo distingue.
- **Porqué:** (1) El coordinador es el único que conoce la especificación completa (comando, entorno, entradas resueltas, salidas declaradas) antes de despachar. (2) Evita lanzar trabajo en Ray, ahorra recursos y latencia. (3) Mantiene al ejecutor simple: no conoce la caché. (4) La consulta es una lectura SQLite rápida; no hay contención porque `WAL` permite lecturas concurrentes.
- **Consecuencia:** el acierto se asienta en la máquina de estados como un resultado, no como un intento: no se diario como `running` (nada se lanzó, así que un reinicio no lo cuenta como en vuelo) y reutiliza `Job.on_result`, que marca la tarea `SUCCEEDED`, deja sus salidas y desbloquea a los dependientes. Una consulta que falla —almacén inalcanzable— se trata como un fallo de caché: se registra como aviso y la tarea se ejecuta, porque la caché puede ahorrar trabajo pero nunca detener un trabajo (S1). La clave la escribe el ejecutor junto con el resultado (D16.6). El historial de intentos registra `cache_hit: true` para trazabilidad (Commit 5).
- **Enmienda (2026-11-23):** la primera redacción situaba la consulta en la transición `PENDING → RUNNING` y la extendía a «nueva ronda o reintento de infraestructura», lo que contradice D16.4; se corrige aquí. También daba por hecho el `cache_hit` del historial, que llega en el Commit 5.

### D16.4 — Solo resultados `SUCCEEDED` (D06.2) entran y salen de la caché

- **Contexto:** D06.2 define `TaskResult.succeeded` como: código de salida 0, sin tiempo límite vencido, sin salidas declaradas ausentes. Es la única definición de «éxito» del sistema.
- **Opciones:** (a) cachear cualquier resultado con código 0; (b) cachear solo `succeeded == True`; (c) cachear también fallos de infraestructura para no reintentarlos.
- **Decisión:** (b) Estricto: `cache_key` se escribe **solo** si `result.succeeded`. La consulta de caché **solo** devuelve resultados con `succeeded == True`.
- **Porqué:** (1) Un fallo de tarea (`TASK`, código ≠ 0) no debe ocultarse: el usuario debe ver el fallo cada vez (R6). (2) Un fallo de infraestructura (`WORKER`, `NETWORK`) no es determinista: reintentar puede tener éxito. Cachearlo convertiría un fallo transitorio en permanente. (3) La regla «el primero que escribe gana» (D12.1) ya protege contra carreras: si dos intentos de la misma ronda terminan, solo el primero escribe; el segundo leerá el resultado en la reconciliación (Fase 6).
- **Consecuencia:** Los reintentos de infraestructura (misma ronda, misma `cache_key`) **no** consultan la caché al reintentar: el coordinador ya sabe que la ronda anterior no tuvo éxito (si la tuvo, la tarea habría terminado). Solo las **nuevas rondas** (reintentos de `TASK` o nuevo trabajo) consultan la caché. La columna `succeeded` de `results` no es una segunda definición de éxito: se escribe con `int(result.succeeded)` (D06.2) y existe solo para que SQLite pueda mantener un índice parcial sobre ella, cosa que no puede hacer sobre el JSON del resultado.

### D16.5 — Acierto de caché: reutilización de salidas sin ejecutar

- **Contexto:** Al haber acierto de caché, el coordinador tiene un `TaskResult` previo con sus `outputs: Dict[str, str]` (ruta → digest). La tarea actual declara las mismas salidas.
- **Opciones:** (a) copiar los blobs del almacén de objetos (CAS) a las rutas de salida del nuevo espacio de trabajo; (b) registrar los digests en el resultado de la tarea actual y dejar que el cliente los descargue del CAS; (c) crear enlaces duros (hard links) entre blobs.
- **Decisión:** (b) El coordinador marca la tarea `SUCCEEDED` con los **mismos digests** de salida que el resultado cachado. Los blobs ya están en el CAS (Fase 3, `cas.py` y actor `BlobStore`). El cliente descarga con `GET /blobs/<digest>` o `artifacts <job> link <dir>`.
- **Porqué:** (1) El CAS es content-addressable: el mismo digest = mismo contenido. No hace falta copiar ni enlazar. (2) El ejecutor no participa: ahorra su arranque y aislamiento. (3) Funciona entre trabajadores distintos: el blob está en el volumen compartido `dpes-data` (Fase 3). (4) El espacio de trabajo del intento inexistente no se crea.
- **Consecuencia:** La vista de la tarea muestra `cache_hit: true`, `execution_id: null`, `outputs` con los digests reutilizados. El historial de intentos registra un intento virtual con `cache_hit: true` y `class: "CACHE_HIT"`.

### D16.6 — Fallo de caché: despacho normal y una sola escritura con la confirmación

- **Contexto:** Si no hay acierto, la tarea se ejecuta normalmente. Al terminar con éxito, su resultado debe estar disponible para futuras tareas idénticas. El punto de confirmación de un resultado es el **ejecutor**: `executor._confirm` lo guarda antes de contestar y reintenta si el almacén no responde (plan/12, D12.2). `ResultLog.save` es `INSERT OR IGNORE` (D12.1), y D02.10 pide que la entrada de caché se escriba en la misma transacción que confirma el éxito, para que no haya un instante con el resultado confirmado y la caché vacía.
- **Opciones:** (a) el ejecutor escribe en la caché al terminar; (b) el coordinador escribe en la caché al recibir `TaskResult` exitoso; (c) un proceso asíncrono popula la caché; (d) el coordinador actualiza la clave después, con una segunda sentencia sobre la fila ya confirmada.
- **Decisión:** (a) La clave viaja dentro del intento: `Attempt` gana `cache_key: str = ""` —ampliación **aditiva** del contrato congelado de `messages.py`, con valor por defecto para no romper a quien construye intentos sin caché—; el coordinador la rellena al despachar (`dataclasses.replace`) y `executor._confirm` la pasa a `store.save_result(key, result, cache_key)` del actor `BlobStore`. Una sola sentencia escribe el resultado de la ronda y, si tuvo éxito, su clave (D16.2). Una clave vacía —tarea con `cache: false`, o una consulta que no procede— no registra nada: `save` la descarta.
- **Porqué:** (1) Es la única forma de que la clave y la confirmación sean la misma escritura sin mover el punto de confirmación, que es lo que piden D02.10 y D12.2. (2) (b) era lo primero que se escribió aquí y no funciona con `INSERT OR IGNORE`: el ejecutor ya guardó la fila, y un segundo guardado del coordinador no añadiría la clave, así que la caché no se poblaría nunca. (3) (d) sí añadiría la clave, pero deja una ventana con el resultado confirmado y la caché no, y hay que repetirla en la ruta de reconciliación. (4) (c) necesita otro proceso y otra cola para una escritura que cabe en la que ya hay. (5) El campo va al final y con valor por defecto: los intentos que ya existen se siguen construyendo igual, y el diario no guarda intentos, así que no hay migración.
- **Consecuencia:** `messages.Attempt` gana el campo; `executor.execute` no cambia de firma; los dobles del almacén de las pruebas aceptan el tercer argumento opcional. La reconciliación no necesita nada nuevo: encuentra la clave en la misma fila que el resultado. La población de la caché es efecto colateral del guardado exitoso, así que un fallo de tarea o de infraestructura nunca la puebla (D16.4).
- **Enmienda (2026-11-23):** la primera redacción decidía (b) —«el coordinador escribe»— y afirmaba que no habría estado intermedio. Se cambia a (a) por lo dicho en el porqué (2); (b) se conserva entre las opciones descartadas y se añade (d), que no estaba escrita.

### D16.7 — La caché es global al clúster, no por trabajador

- **Contexto:** Dos trabajadores distintos pueden ejecutar tareas con la misma especificación. El resultado debe ser reutilizable sin importar quién lo produjo.
- **Opciones:** (a) incluir `worker_id` en la `cache_key`; (b) `cache_key` solo de la especificación (comando, entorno, entradas, salidas declaradas y plataforma).
- **Decisión:** (b) La `cache_key` **no** incluye identidad de trabajador, ni recursos, ni nodo. Solo la especificación pura de la tarea, con la plataforma (D16.1).
- **Porqué:** (1) El aislamiento (Fase 3, `sandbox.py`) garantiza que el comando ve el mismo sistema de archivos, mismo entorno, mismas entradas. Si el comando es determinista, el resultado lo es. (2) Los recursos (CPU, memoria, GPU) afectan rendimiento, no corrección. (3) Incluir trabajador fragmentaría la caché y anularía R9. (4) Si un comando **no** es determinista (ej. usa fecha/hora, aleatoriedad), el usuario debe declararlo como entrada (ej. `--seed=123`) o aceptar que no cachea.
- **Consecuencia:** Un resultado compilado en `worker-1` (con GPU) sirve para `worker-2` (sin GPU) si la especificación es idéntica. El sistema no valida que los recursos coincidan; el usuario controla la determinismo con las entradas.

### D16.8 — Métricas de caché observables en la vista del trabajo

- **Contexto:** El usuario debe poder ver si la caché está funcionando: aciertos, fallos, bytes ahorrados.
- **Opciones:** (a) logs estructurados (D10.4); (b) contadores en la vista del trabajo (`job.view()`); (c) métricas Prometheus.
- **Decisión:** (b) La vista del trabajo (`JobView`) gana `cache_stats: { hits: int, misses: int, bytes_saved: int }`. Cada tarea que consulta la caché incrementa `hits` o `misses`. `bytes_saved` suma el tamaño de los blobs de salida reutilizados (obtenido del CAS).
- **Porqué:** (1) Consistente con el resto de la visibilidad del sistema (estado, intentos, eventos). (2) No requiere infraestructura de métricas externa. (3) `bytes_saved` da una idea concreta del beneficio.
- **Consecuencia:** `scheduler.py` actualiza los contadores al consultar la caché. `coordinator.py` los expone en `get_job(job_id)`. La CLI `dpes.cli cache-stats <job>` (nueva) los muestra.

### D16.9 — La deduplicación en vuelo (F9.6) queda fuera de C7

- **Contexto:** F9.6 (prioridad C) pide que, si dos tareas idénticas están en vuelo, la segunda espere a la primera. La caché de esta fase solo actúa sobre resultados ya guardados: las dos tareas concurrentes ejecutarían por separado y la segunda escribiría su propio resultado, de modo que el trabajo se duplica. La caché no lo impide porque aún no había nada guardado cuando la segunda consultó.
- **Opciones:** (a) implementarla dentro de C7 con un mapa `action_key → resultado pendiente` en el coordinador; (b) dejarla documentada como extensión y cerrar C7 sin ella; (c) descartarla.
- **Decisión:** (b) Queda documentada como extensión E1 en [17-futuras-extensiones.md](17-futuras-extensiones.md), con diseño, riesgos y planificación.
- **Porqué:** es la primera candidata a recorte (D03.6) y no afecta a la corrección: sin ella el resultado final es el mismo, solo se gasta trabajo redundante. El mapa en vuelo añade estado efímero al actor, tiempos de espera, cancelación y pruebas de concurrencia; C7 ya ocupa sus días (corte el 28 de noviembre). Además, el escenario que la justifica —muchos clientes enviando lo mismo a la vez— no aparece en la demostración.
- **Consecuencia:** R9 se cumple con F9.1–F9.5. Cuando se aborde E1, reutilizará `action_key`, porque dos tareas solo comparten ejecución si comparten clave.

## 3. Interfaces nuevas / modificadas

### `dpes/store.py` (extiende Fase 6)

```python
# Esquema extendido (la fila del resultado es también la entrada de caché)
# results: job, task, round, result, cache_key TEXT, succeeded INTEGER
# Índice: CREATE INDEX idx_results_cache_key ON results(cache_key)
#         WHERE succeeded = 1                            -- parcial y no único (D16.2)

def save(self, key: ResultKey, result: TaskResult,
         cache_key: Optional[str] = None) -> TaskResult:
    """Confirma el resultado de la ronda y, si tuvo éxito, lo deja alcanzable
       por cache_key en la misma sentencia. Devuelve el ganador."""

def get_by_cache_key(self, cache_key: str) -> Optional[TaskResult]:
    """El resultado exitoso registrado con esta clave, o None.
       Una clave vacía no encuentra nada."""
```

### `dpes/coordinator.py` — Commit 4 (la consulta)

```python
async def _run(self, job, launch):
    """Consulta la caché antes de que el intento exista en Ray."""
    attempt = launch.attempt
    key = self._action_key(job, attempt)   # "" si la tarea se excluyó (F9.5)
    if key and not launch.retry:           # un reintento de la misma ronda no
        cached = await self._cached(attempt, key)   # tiene éxito que buscar
        if cached is not None:
            await self._settle(job, attempt, cached, hit=True,
                               note=f"cache hit: {cached.execution_id}")
            return
    ...
    outcome = await self._outcome(job, attempt, key)  # el intento viaja con la clave

def _action_key(self, job, attempt) -> str:
    """action_key(comando, entorno, entradas, salidas, platform_id),
       o "" si la tarea declara cache: false."""

async def _cached(self, attempt, key) -> Optional[TaskResult]:
    """store.cached(key), o None si el almacén no contesta en CALL_TIMEOUT_S."""

# actor BlobStore
def cached(self, cache_key: str) -> Optional[TaskResult]:
    """El éxito confirmado registrado con esta clave (get_by_cache_key)."""
```

El acierto reutiliza `Job.on_result`: la tarea pasa a `SUCCEEDED` con las salidas
guardadas y sus dependientes se desbloquean como con cualquier resultado (D16.3).

### `dpes/messages.py` — ampliación aditiva de `Attempt` (D16.6)

```python
class Attempt:
    ...
    timeout_s: float
    cache_key: str = ""   # "" si la tarea no usa la caché
```

### `dpes/scheduler.py` — `Launch`

```python
class Launch:
    attempt: Attempt
    delay_s: float = 0.0
    retry: bool = False   # reintento de la misma ronda: no se consulta (D16.4)
```

### `dpes/spec.py` — Commit 4 (F9.5)

```python
class TaskSpec:
    ...
    cache: bool = True    # "cache": false en el JSON lo desactiva por tarea
```

### `dpes/hashing.py` (extiende la Fase 3) — Commit 2

```python
ACTION_KEY_VERSION = 1

def action_key(command: Sequence[str], env: Mapping[str, str],
               inputs: Mapping[str, str], outputs: Sequence[str],
               platform_id: str,
               key_format_version: int = ACTION_KEY_VERSION) -> str:
    """SHA-256 hex del JSON canónico del trabajo (D16.1).
       Claves ordenadas, sin espacios, UTF-8; salidas ordenadas.
       Devuelve 64 hexadecimales en minúscula, sin prefijo."""
```

El comando conserva su orden (es `argv`, D01.3); `env` e `inputs` se ordenan por
clave al serializar; `outputs` se ordenan porque el orden de declaración no
cambia lo que el comando produce. Una plataforma vacía se rechaza con
`ValueError`: dejaría que una clave de otra cadena de herramientas acertara.

### `dpes/spec.py` (nuevo campo en JobView) — Commit 5

```python
@dataclass(frozen=True)
class JobView:
    ...
    cache_stats: CacheStats  # hits, misses, bytes_saved
```

## 4. Pruebas

| Nivel | Fichero | Qué comprueba |
|-------|---------|---------------|
| Unitarias | `tests/unit/test_hashing.py` | **Commit 2**: `action_key` determinista; cambia con el comando (y su orden), el entorno, un digest de entrada, una salida, la plataforma o la versión; el orden de las claves del JSON y el de las salidas no lo cambia; plataforma vacía rechazada; 64 hexadecimales sin prefijo |
| Unitarias | `tests/unit/test_store.py` | **Commit 3**: `get_by_cache_key` no encuentra nada antes de guardar; encuentra un éxito guardado con clave; nunca encuentra un resultado sin éxito ni una clave vacía; la ronda confirmada conserva la clave del intento que ganó; dos tareas con el mismo trabajo conservan cada una su fila; el acierto sobrevive a reabrir; la tabla registra `succeeded`; una fila nunca se ve sin su clave y un guardado que falla no deja fila; una tabla anterior gana las columnas —también si el cierre se interrumpió entre las dos— y sus filas siguen ahí |
| Unitarias | `tests/unit/test_executor.py` | **Commit 4**: la clave del intento se guarda con el resultado en una sola escritura; un intento sin clave guarda sin ella |
| Unitarias | `tests/unit/test_spec.py` | **Commit 4**: `cache` es verdadero por defecto; `false` lo desactiva; otro tipo se rechaza |
| Unitarias | `tests/unit/test_scheduler.py` | **Commit 4**: `Launch.retry` es verdadero solo en el reintento de la misma ronda |
| Unitarias | `tests/unit/test_scheduler_cache.py` | Acierto: tarea marcada SUCCEEDED sin intento, outputs reutilizados, cache_stats.hits++; Fallo: intento creado, cache_stats.misses++ (Commit 5) |
| Integración | `tests/integration/test_cache.py` | **Commit 4**: el mismo trabajo dos veces ejecuta solo la primera (se mide por los bytes de log del intento); el acierto devuelve los mismos digests y el blob sigue en el CAS; dos tareas idénticas en trabajadores distintos, una sola ejecuta; cambiar una entrada reejecuta solo esa tarea; el dependiente reejecuta si su artefacto cambia y se sirve si la fuente reejecuta y produce el mismo; un fallo nunca se sirve; `cache: false` ejecuta siempre y no deja nada reutilizable; un acierto sobrevive a un reinicio del coordinador |
| Extremo a extremo | `tests/e2e/test_cache.py` | **Corte C7**: trabajo con dos tareas idénticas consecutivas; segunda termina instantáneamente, sin logs de ejecutor, artifacts correctos |

## 5. Actualización de trazabilidad

Al cerrar esta fase, actualizar `plan/00-contexto-y-restricciones.md`:

- R9 → **cumplido** (C7 en verde).
- Decisiones abiertas afectadas: ninguna nueva.
- Riesgos: comando no determinístico sin declararlo como entrada → acierto de caché incorrecto (mitigado: documentación y F1.3).

## 6. Notas de implementación

- **Orden de archivos:** 1) `hashing.py` (clave de acción), 2) `store.py` (esquema y consultas), 3) `scheduler.py` y `spec.py` (`Launch.retry`, `cache: false`), 4) `coordinator.py` (la consulta antes de despachar) y `executor.py` (la clave con la confirmación, D16.6), 5) `spec.py` (cache_stats en la vista), 6) `cli.py` (comando `cache-stats`).
- **De dónde sale `platform_id`:** es configuración del coordinador (imagen de trabajadores y cadena de herramientas), no algo que calcule el ejecutor: así dos trabajadores de la misma imagen producen la misma clave. Se lee de `DPES_PLATFORM_ID`, con `linux/amd64:gcc` por defecto —la imagen `dpes:dev`—, y **sube** cuando cambia la cadena de herramientas, porque las claves de otra no deben acertar. Commit 4 lo pasa a `action_key` y lo escribe en el evento de arranque.
- **Migración BD:** al abrirse, `store.py` mira `PRAGMA table_info(results)` y añade `cache_key` y `succeeded` si faltan; `succeeded` se rellena leyendo cada fila, para no afirmar que un éxito falló, y las filas viejas quedan sin clave: se confirmaron antes de que hubiera caché y un resultado no guarda el comando con el que se puede reconstruir. `CREATE INDEX IF NOT EXISTS` lo hace idempotente.
- **Las pruebas que observan una ejecución dicen `cache: false`:** `tests/e2e` y `tests/host` miran el log, los tiempos, el `hostname` o un fallo inyectado de un intento concreto. Sin la exclusión, un comando ya ejecutado —también en la pasada anterior, porque el volumen `dpes-data` sobrevive— devolvería un acierto y la prueba mediría otra cosa. Es el uso para el que existe F9.5. `tests/host` no lo necesita porque cada prueba mete un marcador único en el comando, así que su clave cambia en cada ejecución.
- **Acierto y `bytes_saved`:** `cas.py` gana `get_size(digest) -> int` en el Commit 5, que es donde la vista gana `cache_stats` (D16.8).
- **Sin librerías nuevas:** solo `hashlib` (stdlib), `sqlite3` (stdlib), `dataclasses` (stdlib).