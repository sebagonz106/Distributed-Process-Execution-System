# 16 — Fase 7: caché basada en contenido

- **Fecha:** 2026-11-25
- **Estado:** en redacción
- **Paso:** Fase 7 de [03-fases-y-dependencias.md](03-fases-y-dependencias.md)
- **Requisito:** R9 — Caché basada en contenido para evitar trabajo idéntico cuando las entradas no cambian
- **Corte:** C7 — una tarea cuyo comando, entorno, entradas y salidas declaradas son idénticos a una ronda anterior exitosa devuelve el resultado guardado sin ejecutar nada

## 1. Alcance y estado

| Feature | Qué | Estado |
|---------|-----|--------|
| F9.1 | Clave de caché: hash determinista de (comando, entorno, entradas, salidas declaradas) | Pendiente |
| F9.2 | Consulta atómica en el almacén antes de despachar | Pendiente |
| F9.3 | Solo resultados `SUCCEEDED` (D06.2) entran y salen de la caché | Pendiente |
| F9.4 | Acierto: el coordinador marca la tarea `SUCCEEDED` y reutiliza salidas | Pendiente |
| F9.5 | Fallo: despacho normal, el resultado nuevo puede poblar la caché | Pendiente |
| F9.6 | Invalidez por cambio de comando, entorno, entradas o salidas declaradas | Pendiente |
| F9.7 | La caché no distingue trabajador: el resultado es portable | Pendiente |
| F9.8 | Métrica observable de aciertos/fallos en la vista del trabajo | Pendiente |

## 2. Decisiones

### D17.1 — La clave de caché es un hash de contenido, no una etiqueta

- **Contexto:** R9 pide evitar trabajo idéntico cuando las entradas no cambian. La especificación de una tarea (F1.1) incluye comando, variables de entorno, entradas (ruta → digest) y salidas declaradas. Dos tareas con la misma especificación deben producir el mismo resultado.
- **Opciones:** (a) una clave compuesta por los campos en bruto (JSON canónico); (b) un hash criptográfico (SHA-256) de la serialización canónica; (c) un identificador asignado por el usuario.
- **Decisión:** (b) SHA-256 de la serialización canónica de `(command, env, inputs, outputs_declared)`. La serialización usa JSON con claves ordenadas, sin espacios, codificación UTF-8. La clave es `cache:<hex>`.
- **Porqué:** (a) hace las claves largas y frágiles ante cambios irrelevantes de formato; (c) traslada la corrección al usuario y rompe la garantía de «entradas idénticas → mismo trabajo». Un hash fijo de 64 hexadecimales es compacto, determinista y no requiere coordinación central para asignar IDs.
- **Consecuencia:** el módulo `dpes/hashing.py` (creado en la Fase 3, D11.1) ya provee `hash_spec(spec: TaskSpec) -> str`. La Fase 7 lo reutiliza; no hay código nuevo de hashing.

### D17.2 — La caché vive en el almacén de resultados (`dpes/store.py`), no en un componente aparte

- **Contexto:** La Fase 6 (D12.1) introdujo `dpes/store.py` con SQLite y la regla «el primero que escribe gana» para guardar `TaskResult` bajo la clave `(job, task, round)`. La caché necesita consultar «¿existe un resultado exitoso para esta especificación?» y, en caso afirmativo, leer sus salidas.
- **Opciones:** (a) tabla `cache` separada con clave `cache_key → result_key`; (b) reutilizar la tabla `results` existente añadiendo un índice por `cache_key`; (c) un almacén de clave-valor distinto (Redis, etcd).
- **Decisión:** (b) Extender `store.py` con una columna `cache_key` en la tabla `results` y un índice único parcial `WHERE succeeded = 1`. La consulta de caché es `SELECT * FROM results WHERE cache_key = ? AND succeeded = 1 LIMIT 1`.
- **Porqué:** (1) Un solo almacén, un solo esquema, una sola transacción para «guardar resultado + poblar caché». (2) La regla «el primero que escribe gana» (D12.1) ya resuelve la carrera entre dos trabajadores que terminan la misma ronda: solo uno escribe, el otro leerá ese resultado en su reconciliación. (3) No añade dependencias externas (cumple regla 3 del CLAUDE.md). (4) SQLite con `WAL` soporta lecturas concurrentes mientras se escribe.
- **Consecuencia:** `store.py` gana `get_by_cache_key(cache_key: str) -> Optional[TaskResult]` y `save_result` escribe `cache_key` cuando `result.succeeded`. El índice parcial garantiza que solo resultados exitosos son cachables.

### D17.3 — Consulta de caché atómica en el coordinador, antes de crear el intento

- **Contexto:** El coordinador decide qué tarea despachar (Fase 2). Antes de lanzar un intento, debe saber si ya existe un resultado en caché para la especificación de esa ronda.
- **Opciones:** (a) el ejecutor consulta la caché al arrancar; (b) el coordinador consulta antes de enviar el `Attempt`; (c) el almacén intercepta el `Attempt` y responde con el resultado si existe.
- **Decisión:** (b) El coordinador, en su máquina de estados, al pasar una tarea de `PENDING` a `RUNNING` (nueva ronda o reintento de infraestructura), llama a `store.get_by_cache_key(cache_key)`. Si hay acierto, marca la tarea `SUCCEEDED` con ese resultado y **no crea intento**.
- **Porqué:** (1) El coordinador es el único que conoce la especificación completa (comando, entorno, entradas resueltas, salidas declaradas) antes de despachar. (2) Evita lanzar trabajo en Ray, ahorra recursos y latencia. (3) Mantiene al ejecutor simple: no conoce la caché. (4) La consulta es una lectura SQLite rápida; no hay contención porque `WAL` permite lecturas concurrentes.
- **Consecuencia:** La transición `PENDING → RUNNING` en `scheduler.py` (o `coordinator.py`) se bifurca: acierto → `SUCCEEDED` directo; fallo → crea `Attempt` y pasa a `RUNNING` normal. El historial de intentos registra `cache_hit: true` para trazabilidad.

### D17.4 — Solo resultados `SUCCEEDED` (D06.2) entran y salen de la caché

- **Contexto:** D06.2 define `TaskResult.succeeded` como: código de salida 0, sin tiempo límite vencido, sin salidas declaradas ausentes. Es la única definición de «éxito» del sistema.
- **Opciones:** (a) cachear cualquier resultado con código 0; (b) cachear solo `succeeded == True`; (c) cachear también fallos de infraestructura para no reintentarlos.
- **Decisión:** (b) Estricto: `cache_key` se escribe **solo** si `result.succeeded`. La consulta de caché **solo** devuelve resultados con `succeeded == True`.
- **Porqué:** (1) Un fallo de tarea (`TASK`, código ≠ 0) no debe ocultarse: el usuario debe ver el fallo cada vez (R6). (2) Un fallo de infraestructura (`WORKER`, `NETWORK`) no es determinista: reintentar puede tener éxito. Cachearlo convertiría un fallo transitorio en permanente. (3) La regla «el primero que escribe gana» (D12.1) ya protege contra carreras: si dos intentos de la misma ronda terminan, solo el primero escribe; el segundo leerá el resultado en la reconciliación (Fase 6).
- **Consecuencia:** Los reintentos de infraestructura (misma ronda, misma `cache_key`) **no** consultan la caché al reintentar: el coordinador ya sabe que la ronda anterior no tuvo éxito (si la tuvo, la tarea habría terminado). Solo las **nuevas rondas** (reintentos de `TASK` o nuevo trabajo) consultan la caché.

### D17.5 — Acierto de caché: reutilización de salidas sin ejecutar

- **Contexto:** Al haber acierto de caché, el coordinador tiene un `TaskResult` previo con sus `outputs: Dict[str, str]` (ruta → digest). La tarea actual declara las mismas salidas.
- **Opciones:** (a) copiar los blobs del almacén de objetos (CAS) a las rutas de salida del nuevo espacio de trabajo; (b) registrar los digests en el resultado de la tarea actual y dejar que el cliente los descargue del CAS; (c) crear enlaces duros (hard links) entre blobs.
- **Decisión:** (b) El coordinador marca la tarea `SUCCEEDED` con los **mismos digests** de salida que el resultado cachado. Los blobs ya están en el CAS (Fase 3, `cas.py` y actor `BlobStore`). El cliente descarga con `GET /blobs/<digest>` o `artifacts <job> link <dir>`.
- **Porqué:** (1) El CAS es content-addressable: el mismo digest = mismo contenido. No hace falta copiar ni enlazar. (2) El ejecutor no participa: ahorra su arranque y aislamiento. (3) Funciona entre trabajadores distintos: el blob está en el volumen compartido `dpes-data` (Fase 3). (4) El espacio de trabajo del intento inexistente no se crea.
- **Consecuencia:** La vista de la tarea muestra `cache_hit: true`, `execution_id: null`, `outputs` con los digests reutilizados. El historial de intentos registra un intento virtual con `cache_hit: true` y `class: "CACHE_HIT"`.

### D17.6 — Fallo de caché: despacho normal, resultado nuevo puede poblar la caché

- **Contexto:** Si no hay acierto, la tarea se ejecuta normalmente. Al terminar con éxito, su resultado debe estar disponible para futuras tareas idénticas.
- **Opciones:** (a) el ejecutor escribe en la caché al terminar; (b) el coordinador escribe en la caché al recibir `TaskResult` exitoso; (c) un proceso asíncrono popula la caché.
- **Decisión:** (b) El coordinador, al procesar `TaskResult` con `succeeded == True` en la reconciliación (Fase 6) o en la ruta normal, llama a `store.save_result(result)` que ya escribe `cache_key` (D17.2). Es parte de la misma transacción que guarda el resultado por `(job, task, round)`.
- **Porqué:** (1) El coordinador es quien conoce la `cache_key` (la construyó para consultar). (2) Una sola escritura atómica: resultado por ronda + entrada de caché. (3) Si el guardado falla (ej. contención SQLite), la transacción revierte y el coordinador reintentará la reconciliación (Fase 6).
- **Consecuencia:** No hay lógica de caché en el ejecutor. La población es efecto colateral del guardado exitoso.

### D17.7 — La caché es global al clúster, no por trabajador

- **Contexto:** Dos trabajadores distintos pueden ejecutar tareas con la misma especificación. El resultado debe ser reutilizable sin importar quién lo produjo.
- **Opciones:** (a) incluir `worker_id` en la `cache_key`; (b) `cache_key` solo de la especificación (comando, entorno, entradas, salidas declaradas).
- **Decisión:** (b) La `cache_key` **no** incluye identidad de trabajador, ni recursos, ni nodo. Solo la especificación pura de la tarea.
- **Porqué:** (1) El aislamiento (Fase 3, `sandbox.py`) garantiza que el comando ve el mismo sistema de archivos, mismo entorno, mismas entradas. Si el comando es determinista, el resultado lo es. (2) Los recursos (CPU, memoria, GPU) afectan rendimiento, no corrección. (3) Incluir trabajador fragmentaría la caché y anularía R9. (4) Si un comando **no** es determinista (ej. usa fecha/hora, aleatoriedad), el usuario debe declararlo como entrada (ej. `--seed=123`) o aceptar que no cachea.
- **Consecuencia:** Un resultado compilado en `worker-1` (con GPU) sirve para `worker-2` (sin GPU) si la especificación es idéntica. El sistema no valida que los recursos coincidan; el usuario controla la determinismo con las entradas.

### D17.8 — Métricas de caché observables en la vista del trabajo

- **Contexto:** El usuario debe poder ver si la caché está funcionando: aciertos, fallos, bytes ahorrados.
- **Opciones:** (a) logs estructurados (D10.4); (b) contadores en la vista del trabajo (`job.view()`); (c) métricas Prometheus.
- **Decisión:** (b) La vista del trabajo (`JobView`) gana `cache_stats: { hits: int, misses: int, bytes_saved: int }`. Cada tarea que consulta la caché incrementa `hits` o `misses`. `bytes_saved` suma el tamaño de los blobs de salida reutilizados (obtenido del CAS).
- **Porqué:** (1) Consistente con el resto de la visibilidad del sistema (estado, intentos, eventos). (2) No requiere infraestructura de métricas externa. (3) `bytes_saved` da una idea concreta del beneficio.
- **Consecuencia:** `scheduler.py` actualiza los contadores al consultar la caché. `coordinator.py` los expone en `get_job(job_id)`. La CLI `dpes.cli cache-stats <job>` (nueva) los muestra.

## 3. Interfaces nuevas / modificadas

### `dpes/store.py` (extiende Fase 6)

```python
# Esquema extendido
# results table: ..., cache_key TEXT, succeeded BOOLEAN
# Índice: CREATE UNIQUE INDEX idx_cache_key ON results(cache_key) WHERE succeeded = 1

def get_by_cache_key(self, cache_key: str) -> Optional[TaskResult]:
    """Devuelve el primer resultado exitoso con esta cache_key, o None."""

def save_result(self, result: TaskResult, cache_key: str) -> None:
    """Guarda resultado por (job, task, round) y, si succeeded, por cache_key.
       Transacción única: o las dos escrituras o ninguna."""
```

### `dpes/scheduler.py` / `dpes/coordinator.py` (lógica de despacho)

```python
def _try_cache(self, task: Task, round: int, spec: TaskSpec) -> Optional[TaskResult]:
    """Consulta store.get_by_cache_key(hash_spec(spec)).
       Si acierto: actualiza cache_stats, marca tarea SUCCEEDED, devuelve resultado.
       Si fallo: actualiza cache_stats, devuelve None."""
```

### `dpes/hashing.py` (ya existe, Fase 3)

```python
def hash_spec(spec: TaskSpec) -> str:
    """SHA-256 hex de JSON canónico de (command, env, inputs, outputs_declared).
       Devuelve 'cache:<hex>'."""
```

### `dpes/spec.py` (nuevo campo en JobView)

```python
@dataclass(frozen=True)
class JobView:
    ...
    cache_stats: CacheStats  # hits, misses, bytes_saved
```

## 4. Pruebas

| Nivel | Fichero | Qué comprueba |
|-------|---------|---------------|
| Unitarias | `tests/unit/test_hashing.py` | `hash_spec` determinista, mismo spec → mismo hash, cambio mínimo → hash distinto |
| Unitarias | `tests/unit/test_store.py` | `get_by_cache_key` devuelve None si no hay éxito; devuelve resultado si succeeded; índice parcial rechaza duplicados exitosos; transacción atómica save_result |
| Unitarias | `tests/unit/test_scheduler_cache.py` | Acierto: tarea marcada SUCCEEDED sin intento, outputs reutilizados, cache_stats.hits++; Fallo: intento creado, cache_stats.misses++; Reintento de infraestructura (misma ronda) no consulta caché |
| Integración | `tests/integration/test_cache.py` | Dos tareas idénticas en trabajadores distintos: segunda acierta en caché; blob reutilizado desde CAS; vista muestra cache_hit |
| Extremo a extremo | `tests/e2e/test_cache.py` | **Corte C7**: trabajo con dos tareas idénticas consecutivas; segunda termina instantáneamente, sin logs de ejecutor, artifacts correctos |

## 5. Actualización de trazabilidad

Al cerrar esta fase, actualizar `plan/00-contexto-y-restricciones.md`:

- R9 → **cumplido** (C7 en verde).
- Decisiones abiertas afectadas: ninguna nueva.
- Riesgos: comando no determinístico sin declararlo como entrada → acierto de caché incorrecto (mitigado: documentación y F1.3).

## 6. Notas de implementación

- **Orden de archivos:** 1) `store.py` (esquema + consultas), 2) `scheduler.py` (consulta antes de despachar), 3) `coordinator.py` (población al guardar), 4) `spec.py` (cache_stats en vista), 5) `cli.py` (comando `cache-stats`).
- **Migración BD:** `store.py` ejecuta `ALTER TABLE results ADD COLUMN cache_key TEXT` + `CREATE UNIQUE INDEX ...` al inicializar. `IF NOT EXISTS` lo hace idempotente.
- **Tamaño de blobs para `bytes_saved`:** `cas.py` expone `get_size(digest) -> int`. El coordinador suma `sum(cas.get_size(d) for d in result.outputs.values())` al contabilizar acierto.
- **Sin librerías nuevas:** solo `hashlib` (stdlib), `sqlite3` (stdlib), `dataclasses` (stdlib).