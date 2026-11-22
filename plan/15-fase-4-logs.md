# 15 — Fase 4: logs de tareas en vivo y historial de eventos

- **Fecha:** 2026-11-18
- **Estado:** en curso (Commit 2: actor `LogStore` y su núcleo en disco)
- **Paso:** Fase 4 de [03-fases-y-dependencias.md](03-fases-y-dependencias.md)
- **Requisito:** R5 (registro y transmisión de logs)
- **Corte:** C4 — `logs -f` muestra líneas mientras la tarea corre; tras matar el worker a mitad, lo emitido sigue consultable; `events` muestra historial con execution IDs, transiciones, clases de fallo y esperas de reintento

## 1. Alcance

| Feature | Qué | Estado |
|---------|-----|--------|
| F5.1 | Captura de stdout/stderr de la tarea | ✅ Ya existe (output_tail 64 KiB, D07.14) |
| F5.2 | Envío incremental durante la ejecución, en fragmentos numerados | 🟡 Este documento |
| F5.3 | Consulta por desplazamiento y seguimiento en vivo (`logs -f`) | 🟡 Este documento |
| F5.4 | Lo transmitido sobrevive a la muerte del worker | 🟡 Este documento |
| F5.5 | Historial de eventos del trabajo: transiciones, intentos, clase de fallo y decisión de reintento, correlacionados por execution ID | 🟡 Este documento |
| F5.6 | En acierto de caché se sirve el log de la ejecución original | 🟡 Fase 7 (depende de este) |

---

## 2. Decisiones

### D15.1 — Formato de fragmento de log y numeración

**Contexto:** El ejecutor debe enviar la salida de la tarea (stdout/stderr combinados) al almacén de logs en fragmentos durante la ejecución. Cada fragmento debe ser identificable de forma única para permitir idempotencia y reordenamiento.

**Opciones consideradas:**
1. **Secuencia global por execution_id:** Entero monotonico (1, 2, 3...) por intento. Simple, permite detectar huecos y duplicados.
2. **Timestamp + offset:** Pareja (timestamp_ms, byte_offset). Más información pero complejidad de comparación y posibles colisiones.
3. **UUID por fragmento:** Identificador único aleatorio. Garantiza unicidad pero no permite ordenamiento natural ni detección de huecos simple.

**Decisión:** Opción 1 — secuencia global por execution_id (enteros 1, 2, 3...).

**Porqué:**
- La idempotencia en el almacén (D15.2) requiere una clave simple: `(execution_id, seq)` como clave primaria.
- Permite al cliente pedir "desde seq N" y al almacén ignorar duplicados exactos.
- El orden natural de enteros coincide con el orden causal de emisión.
- Mínima sobrecarga: 4-8 bytes por fragmento en metadatos.

**Consecuencias:**
- El ejecutor debe mantener contador local por intento y persistirlo si se reiniciara (no aplica: el ejecutor no se reinicia a mitad de intento).
- El almacén debe rechazar/ignorar escrituras con `seq <= max_seq_existente` para ese execution_id.

---

### D15.2 — Persistencia del almacén de logs (LogStore)

**Contexto:** Los fragmentos deben sobrevivir a la muerte del worker (F5.4) y estar disponibles para consulta por offset y streaming en vivo (F5.3). El volumen `dpes-data` en el nodo de control ya existe y persiste entre reinicios de contenedor.

**Opciones consideradas:**
1. **Archivo por intento:** `/var/lib/dpes/logs/{execution_id}.log` — escritura append-only, índice en memoria (mapa seq → byte_offset). Lectura por offset = seek directo.
2. **SQLite:** Tabla `fragments(execution_id, seq, data, offset)`. Transacciones ACID, consultas flexibles, pero más overhead por fragmento.
3. **Archivo por fragmento:** `/var/lib/dpes/logs/{execution_id}/seq_{N}.chunk`. Simple pero muchos inodes y syscalls.

**Decisión:** Opción 1 — un archivo por intento (`{execution_id}.log`) con índice en memoria reconstruido al abrir.

**Porqué:**
- Append-only es el patrón de acceso natural (escritura secuencial, lectura por offset).
- Seek en archivo es O(1) y extremadamente rápido; no hace falta B-tree para este caso de uso.
- Un archivo por intento = limpieza trivial (borrar archivo = borrar logs de un intento).
- Evita overhead de SQLite (parseo SQL, transacciones) para un workload de "write once, read many" simple.
- El volumen `dpes-data` ya está montado en `/var/lib/dpes`; solo crear subdirectorio `logs/`.

**Consecuencias:**
- El índice en memoria (mapa `seq -> file_offset`) se reconstruye escaneando el archivo al abrir (rápido: lectura secuencial una sola vez).
- Tamaño máximo de archivo = salida total de la tarea. Si una tarea produce GBs, el archivo crece sin límite (documentar como límite conocido, F5.4 cubre supervivencia, no rotación).
- El índice también crece con los fragmentos, no solo con los bytes: cada
  fragmento añade una tupla `(seq, desplazamiento en la salida, longitud)` a la
  lista del intento, y esa lista vive en la memoria del actor mientras viva. Con
  el intérprete de CPython 3.13 son ~145 B por fragmento (medido): 1 GiB de
  salida en fragmentos de 4 KiB son ~38 MB de RAM, por intento. Aceptable para
  el alcance —los trabajos de la asignatura producen MB, no GB—, pero es el
  coste que crece si se sube el tamaño de la salida o se baja el del fragmento.
- Nada borra ni rota: los archivos de `/var/lib/dpes/logs` se acumulan para
  siempre, y comparten el volumen `dpes-data` con los blobs, `results.sqlite` y
  `coordinator.sqlite` del mismo nodo de control. Una tarea que imprima en bucle
  puede llenar ese volumen y arrastrar a los demás componentes; hoy el único
  tope es el disco del anfitrión. Retención y rotación quedan fuera de alcance
  (Fase 8 o post-entrega), no porque no se vean, sino porque no las pide R5 y
  añadirlas obliga a decidir política (¿cuánto se guarda?, ¿quién barre?).

**Estructura del archivo:**
```
[header: magic=0x44504553 ("DPES"), version=1, execution_id_len, execution_id]
[fragment_1: seq=1 (u64), data_len=L1 (u32), data[L1]]
[fragment_2: seq=2 (u64), data_len=L2 (u32), data[L2]]
...
```
- Binario, little-endian. Permite parsing rápido y seek exacto.
- `data` = bytes raw de stdout/stderr (UTF-8 válido asumido, pero almacenamos bytes).

---

### D15.3 — Política de emisión desde el ejecutor

**Contexto:** El ejecutor captura stdout/stderr en un archivo temporal (ya implementado en D07.15). Debe leer ese archivo incrementalmente y enviar fragmentos al LogStore sin bloquear la ejecución del comando.

**Opciones consideradas:**
1. **Hilo dedicado (threading.Thread):** Lee el archivo en bucle, envía fragmentos cada N bytes / M líneas / T ms. Simple, estándar en Python.
2. **Asyncio + aiofiles:** Requiere que el ejecutor sea async; hoy es síncrono (D07.17). Cambio mayor.
3. **Polling desde el coordinador:** El coordinador pide fragmentos al ejecutor. Invierte dependencias, añade latencia y complejidad.

**Decisión:** Opción 1 — hilo dedicado (`threading.Thread`) en el ejecutor.

**Porqué:**
- El ejecutor ya es síncrono y no conoce asyncio; un hilo es el cambio mínimo.
- `threading` está en la biblioteca estándar (sin dependencias extra).
- El hilo puede ser daemon: si el proceso principal termina, el hilo muere sin bloquear.
- Permite batching configurable: `flush_bytes=4096`, `flush_lines=100`, `flush_interval_ms=500` (lo que ocurra primero).

**Detalles de implementación:**
```python
class LogEmitter:
    def __init__(self, store_client, execution_id, log_file_path):
        self.store = store_client
        self.eid = execution_id
        self.path = log_file_path
        self.seq = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self): self._thread.start()
    def stop(self, timeout=2.0): self._stop.set(); self._thread.join(timeout)

    def _run(self):
        with open(self.path, 'rb') as f:
            while not self._stop.is_set():
                chunk = f.read(4096)
                if chunk:
                    self.seq += 1
                    self.store.append_fragment(self.eid, self.seq, chunk)
                else:
                    time.sleep(0.1)  # wait for more data
        # EOF: send final marker (empty chunk with seq+1 and EOF flag in metadata?)
```

**Consecuencias:**
- El ejecutor debe importar `threading` (ya lo hace para `signal` en sandbox).
- El cliente de almacenamiento (`_StoreClient`) gana método `append_fragment(execution_id, seq, data)`.
- Si el hilo falla al enviar (red, store caído), loggea warning y reintenta con backoff simple (3 intentos, 0.5s). No bloquea la ejecución del comando.
- El comando y el emisor no pueden compartir un descriptor con una sola posición: el ejecutor crea el archivo de salida, lo deja sin nombre (`unlink` inmediato) y lo lee por un segundo descriptor con posición propia. Sandbox pasa a aceptar el archivo del llamante (`sink`) en vez de crear siempre el suyo.
- Un almacén que no acepta un fragmento ni tras los tres reintentos se da por caído: el emisor deja de enviar el resto de ese intento (un log no puede tumbar un trabajo) y lo declara con un `WARNING` con `execution=` y `lost=`. Reintentar cada fragmento de un almacén muerto solo retrasaría la tarea.
- El ejecutor espera al emisor como mucho `JOIN_S = 3` s, por debajo del margen `RUN_MARGIN_S = 5` s del coordinador (D09.4); si el hilo sigue vivo, es *daemon* y termina solo.

---

### D15.4 — API de logs en la pasarela (Gateway)

**Contexto:** El cliente necesita consultar logs por offset (histórico) y seguirlos en vivo (`logs -f`). La pasarela es el único punto de entrada HTTP (puerto 8000).

**Opciones consideradas:**
1. **Dos endpoints separados:** `GET /logs?offset=N&limit=M` y `GET /logs?follow=1`.
2. **Un endpoint con query params:** `GET /logs?offset=N&limit=M&follow=0/1`.
3. **Range header HTTP estándar:** `GET /logs` con `Range: bytes=START-` y `Accept: text/plain`; streaming con `Transfer-Encoding: chunked`.

**Decisión:** Opción 2 (un endpoint con params) + soporte opcional de `Range` header (Opción 3 como bonus).

**Endpoints:**
```http
# 1. Consulta histórica por offset
GET /jobs/{job_id}/tasks/{task}/logs?offset={n}&limit={m}
→ 200 OK, Content-Type: text/plain
   Body: concatenación de fragmentos desde offset n (bytes), hasta m bytes o EOF
→ 404 si job/task/execution no existe
→ 400 si params inválidos

# 2. Streaming en vivo (logs -f)
GET /jobs/{job_id}/tasks/{task}/logs?follow=1&offset={n}
→ 200 OK, Content-Type: text/plain, Transfer-Encoding: chunked
   Chunks: fragmentos existentes desde offset n, luego nuevos conforme llegan
   Cada chunk = fragmento completo (seq + data) o solo data? → solo data (text/plain)
→ Conexión se cierra cuando: cliente desconecta, tarea termina (EOF), o timeout 5 min sin datos
```

**Formato de respuesta (ambos):**
- `text/plain; charset=utf-8` — bytes directos de stdout/stderr.
- El cliente reconstruye líneas; no añadimos delimitadores extra.

**Soporte `Range` (bonus, no bloqueante):**
```http
GET /jobs/{job_id}/tasks/{task}/logs
Range: bytes=1000-
→ 206 Partial Content, Content-Range: bytes 1000-1999/2000
```
Útil para clientes HTTP genéricos (curl, wget).

**Manejo de desconexión en streaming:**
- El handler detecta `GeneratorExit` / `BrokenPipeError` / `ConnectionResetError`.
- Al detectar, cancela la suscripción al `tail_fragments` del LogStore y libera recursos.
- No deja hilos/corutinas huérfanas.

**Consecuencias:**
- La pasarela necesita acceso al actor `LogStore` (ya tiene acceso al `BlobStore` y `Coordinator`).
- `RAY_DEDUP_LOGS=0` en compose (ya configurado, D10.7) evita que Ray fusione líneas de distintos intentos.

**Refinada por D15.15**: qué intento se sirve cuando la ruta nombra la
tarea, `Range` con 206/416 y `Content-Range` exacto, `Connection: close` con
HTTP/1.1, y hasta dónde llega la limpieza de una desconexión (se libera el hilo
del handler; el generador remoto del almacén no se puede cancelar y termina por
inactividad).

---

### D15.5 — API de historial de eventos en la pasarela

**Contexto:** F5.5 requiere historial de eventos del trabajo: transiciones de estado, intentos, clase de fallo, decisión de reintento, correlacionados por execution ID. Ya existe el historial de intentos en la vista del trabajo (D09.6), pero F5.5 pide un feed cronológico unificado servido por HTTP.

**Decisión:** Endpoint `GET /jobs/{job_id}/events` que devuelve array JSON ordenado cronológicamente.

**Esquema de evento:**
```json
{
  "timestamp": "2026-11-18T21:04:05.123Z",
  "type": "TASK_LAUNCHED | ATTEMPT_SUCCEEDED | ATTEMPT_FAILED | RETRY_SCHEDULED | JOB_FINISHED",
  "execution_id": "a1b2c3d4/compile/0/1",
  "job_id": "a1b2c3d4",
  "task": "compile",
  "payload": {
    "state": "RUNNING",
    "failure_class": "WORKER",
    "retry_in_s": 2.5,
    "reason": "WorkerCrashedError",
    "note": "adopted by reconciliation"
  }
}
```

**Tipos de evento y cuándo se emiten (desde `scheduler.Job`, D15.6):**
| Tipo | Cuándo | Payload clave |
|------|--------|---------------|
| `TASK_LAUNCHED` | `start()` o `_launch()` | `state: "RUNNING"` |
| `ATTEMPT_SUCCEEDED` | `on_result()` con éxito | `state: "SUCCEEDED", exit_code` |
| `ATTEMPT_FAILED` | `on_result()` fallo tarea / `on_failure()` | `failure_class, reason, retry_in_s` |
| `RETRY_SCHEDULED` | `_failed()` programa reintento | `failure_class, retry_in_s, next_execution_id` |
| `TASK_SKIPPED` | `_skip_dependents_of()` (añadido por D15.17) | `state: "SKIPPED", reason` |
| `JOB_FINISHED` | `Job.finished()`, llamado por el coordinador | `state: "SUCCEEDED|FAILED|ERROR", counts` |

**Persistencia:** Los eventos viven en memoria en `scheduler.Job._events` y se reconstruyen al reiniciar el coordinador vía journal (D14.1). El endpoint `events()` del actor coordinador lee `job._events`. D15.16 fija cómo se conserva la hora y D15.17 la forma final del evento.

**Consecuencias:**
- Los eventos son **datos**, no logs operativos (D10.4). Los módulos puros no logean; el coordinador emite eventos como estructuras de datos.
- Timestamp en ISO 8601 UTC con milisegundos (generado en el coordinador al emitir, en hora de pared; D15.16).
- El endpoint es de solo lectura; no requiere idempotency key.

**Al implementar (2026-11-20):**
- El instante lo pone el coordinador; **el texto ISO lo escribe la pasarela**
  (`_wire`/`_iso`). El actor devuelve el flotante tal cual porque es lo que el
  diario rejuega y lo que las pruebas comparan de forma exacta: la
  representación de una hora es asunto del transporte, no del dato. La frase
  «generado en el coordinador» de arriba se refiere al instante, no al formato.
- Redondear al milisegundo puede saltar al segundo siguiente
  (`1795143281.9996` → `…:42.000Z`): `_iso` redondea a milisegundos enteros y
  deriva la fecha de `millis // 1000`, así que el acarreo es correcto.
- Ruta propia `_EVENTS_PATH`, probada antes que la del estado y con
  `unquote`, como la de logs. Un trabajo conocido sin eventos todavía es
  `[]` (200), no un 404: el 404 se reserva a «ese trabajo no existe».
- La consulta se registra en DEBUG con `job` y `found`, como el estado: los
  clientes la sondean (D10.5). Nunca se registra el contenido de un evento
  (D10.6).
- `_Client.events(job_id)` es una llamada bloqueante más con `CALL_TIMEOUT_S`,
  igual que `status`.

---

### D15.6 — Emisión de eventos desde el scheduler (núcleo puro)

**Contexto:** La máquina de estados pura (`scheduler.Job`) debe emitir eventos de datos (no logs) en cada transición relevante. Estos eventos alimentan el endpoint `/events` (D15.5) y complementan el historial de intentos (D09.6).

**Decisión:** Extender `scheduler.Job` con lista `_events` y método `_record_event(type, execution_id, **payload)`.

**Implementación (forma final, refinada por D15.16 a D15.18):**
```python
@dataclass(frozen=True)
class Event:
    timestamp: float      # hora de pared del reloj inyectado (D15.16)
    type: str
    job_id: str
    task: str | None
    execution_id: str
    payload: dict

class Job:
    def __init__(self, ..., clock=None):
        self._clock = clock or _epoch  # el job_id lo pone el propio Job
        self._events = []
        self._done = False

    def _record_event(self, type, execution_id, task, **payload):
        self._events.append(Event(self._clock(), type, self.job_id, task,
                                  execution_id, payload))

    # Puntos de llamada:
    # _launch()                     -> TASK_LAUNCHED (primer intento y siguientes)
    # on_result() éxito             -> ATTEMPT_SUCCEEDED
    # _failed()                     -> ATTEMPT_FAILED (con o sin presupuesto)
    # _failed() con reintento       -> RETRY_SCHEDULED
    # _skip_dependents_of()         -> TASK_SKIPPED por tarea omitida
    # finished()                    -> JOB_FINISHED (una vez por trabajo)
    # running()                     -> nada (D15.18)
```

**Consecuencias:**
- `scheduler.py` sigue siendo **puro**: no importa `time`, no escribe disco, no usa Ray; el reloj entra por parámetro (D15.16).
- Los tests unitarios verifican: cada transición emite evento correcto, payload completo, orden de inserción.
- El coordinador (actor Ray) expone `events(job_id)` que devuelve `job._events` convertido a JSON serializable.

---

### D15.7 — CLI: comando `logs`

**Contexto:** El usuario necesita consultar logs históricos y seguirlos en vivo desde la línea de comandos.

**Decisión:** Nuevo subcomando `logs` con flags `-f/--follow`, `-o/--offset`, `-n/--lines`.

**Uso:**
```bash
# Histórico desde offset
py -m dpes.cli logs <job_id> <task> -o 1000

# Últimas N líneas (aproximado: offset = tamaño_archivo - N*avg_line)
py -m dpes.cli logs <job_id> <task> -n 100

# Seguimiento en vivo (Ctrl+C para salir)
py -m dpes.cli logs <job_id> <task> -f

# Combinado: desde offset y seguir
py -m dpes.cli logs <job_id> <task> -o 5000 -f
```

**Implementación en `dpes/cli.py`:**
- `follow=False`: `GET /jobs/{id}/tasks/{task}/logs?offset={o}&limit={n}` → imprime cuerpo y sale.
- `follow=True`: `GET /jobs/{id}/tasks/{task}/logs?follow=1&offset={o}` con `stream=True` → itera chunks e imprime en tiempo real. Maneja `KeyboardInterrupt` limpio.

**Códigos de salida:**
- 0: éxito
- 1: error HTTP (4xx, 5xx)
- 2: job/task no encontrado (404)
- 3: error de conexión / gateway inalcanzable

**Al implementar (2026-11-20):**
- Los tres códigos de fallo se resuelven en un solo sitio (`_failure_code`), no
  dentro de cada ruta: el resto de comandos sigue devolviendo 3 para todo y la
  asimetría queda declarada como deuda.
- **`-n` es exacto, no una estimación por bytes.** El plan hablaba de calcular
  `offset = tamaño - N·media_de_línea`, pero el tamaño no está expuesto por HTTP
  y un offset adivinado puede empezar en mitad de una línea. `logs` pide desde
  `-o` hasta el final y se queda con las últimas N líneas; cuesta memoria
  proporcional al log, que es el límite ya conocido de §7. `-n 0` imprime todo.
- `-f` **no** pone plazo propio: una compilación puede estar callada más tiempo
  que cualquier plazo que se invente, y la pasarela cierra el flujo cuando el
  intento cierra su log (D15.4). Ctrl+C sale con 0; una conexión medio abierta
  solo se nota al escribir.
- El flujo se lee con `read1`, que devuelve en cuanto hay bytes (un `read`
  esperaría a llenar el búfer y la salida no sería en vivo), y se decodifica con
  un decodificador incremental de UTF-8: un corte de fragmento puede caer dentro
  de un carácter (D15.4).
- `-n` junto a `-f` y un `-n` negativo se rechazan como error de uso: son
  peticiones contradictorias, no algo que ignorar en silencio.
- Se corrigió de paso una clasificación de la pasarela (Commit 4): `_serve_logs`
  trataba **cualquier** `OSError` como «el cliente se fue», así que un fallo del
  coordinador en esa ruta se quedaba sin respuesta (el cliente veía una conexión
  cerrada) en vez de un 503. Ahora solo `BrokenPipeError` y
  `ConnectionResetError` cuentan como cliente que se fue; lo demás llega como
  503, que es lo que `logs` convierte en su código 1.
- `tests/e2e/gateway_client.cli` acepta `url=` para poder probar el caso «no hay
  pasarela» sin salir del contenedor.

**Consecuencias:**

- **Deuda declarada:** `status`, `artifacts` y `submit` siguen devolviendo 3
  ante cualquier fallo de petición, 404 incluido. Unificarlos con D15.7 es
  trabajo pendiente de un paso posterior; hasta entonces, la diferencia está
  documentada aquí y en el docstring del CLI.
- Un error de uso (por ejemplo `-n` junto a `-f`) sale con **2**, como
  cualquier error de `argparse`; se distingue por el mensaje, no por el número.
  Es una limitación de `argparse` que se asume, no se rodea.
- `logs` no usa `Idempotency-Key`: es solo lectura.

---

### D15.8 — Contrato del cliente de almacenamiento (_StoreClient)

**Contexto:** El ejecutor y la pasarela necesitan hablar con el actor `LogStore`. El patrón existente (`_StoreClient` en `coordinator.py`) envuelve llamadas Ray con timeouts.

**Decisión:** Añadir método `append_fragment(execution_id, seq, data)` a `_StoreClient`.

```python
class _StoreClient:
    # ... existing methods ...
    def append_fragment(self, execution_id: str, seq: int, data: bytes) -> bool:
        reply = self._actor.append_fragment.remote(execution_id, seq, data)
        return ray.get(reply, timeout=CALL_TIMEOUT_S)
```

**Timeout:** `CALL_TIMEOUT_S = 10` (igual que otras llamadas). Si falla, el hilo emisor del ejecutor reintenta 3× con backoff 0.5s (D15.3).

---

### D15.9 — Manejo de deduplicación de logs de Ray

**Contexto:** Ray por defecto deduplica líneas de log que difieren solo en números (D10.7). Los execution IDs contienen números (`job/task/round/attempt`), así que Ray fusionaría líneas de intentos distintos.

**Decisión:** Confirmar `RAY_DEDUP_LOGS=0` en `docker/compose.yaml` para el servicio `head` (ya está, D10.7). Documentar en D15.1 que el almacén usa su propia secuencia, no la de Ray.

**Verificación:** Test unitario en C1 que comprueba que dos intentos distintos con logs idénticos producen líneas separadas en `docker logs dpes-head`.

---

### D15.16 — Reloj de pared inyectado y persistencia de la hora en el diario

**Contexto:** D15.6 propone `time.monotonic()` pero D15.5 pide ISO 8601 UTC; un reloj monótono no se convierte a hora de pared. El diario no guarda timestamps. El reinicio pierde la hora original.

**Decisión:** El coordinador inyecta `clock=time.time` al crear `Job`; el scheduler usa ese reloj (no importa `time`). El diario (`store.Journal`) gana columna `at REAL NOT NULL` con migración `ALTER TABLE events ADD COLUMN at REAL DEFAULT (julianday('now') * 86400000)` al abrir. Los eventos guardan su `at`; el replay los recupera con su hora original.

**Al implementar (2026-11-20):**
- SQLite solo añade una columna con un valor por defecto **constante**: la
  expresión `julianday('now')` que decía esta decisión no es aceptable en
  `ALTER TABLE`. La migración real es
  `ALTER TABLE events ADD COLUMN at REAL NOT NULL DEFAULT 0`, y las filas
  escritas antes de esta decisión se repiten con `at = 0` (la época): su hora
  nunca se registró e inventarla sería mentir. Se detecta con
  `PRAGMA table_info(events)`, así que abrir un diario ya migrado no cambia
  nada.
- El reloj no es «la hora del coordinador» en abstracto: `Journal.append`
  devuelve el `at` que acaba de guardar y el coordinador fija con él el reloj
  mientras ejecuta **esa** llamada (`_Clock`, `_journaled`). Así el evento en
  vivo y el reconstruido son idénticos, no solo parecidos.
- Consecuencia: todos los eventos que produce una misma llamada comparten el
  instante (un `on_result` puede desbloquear varios `TASK_LAUNCHED`). El orden
  del feed es el orden de inserción, no el valor de `timestamp`.
- El reloj por defecto de `Job` es `_epoch` (`0.0`), no `time.time`: el módulo
  sigue sin importar `time` y las pruebas que no miden tiempo no dependen del
  reloj de la máquina.

---

### D15.17 — Forma del evento, JOB_FINISHED diarizado y TASK_SKIPPED

**Contexto:** D15.5 pide `job_id` y `task` en el JSON; D15.6 solo tiene `execution_id`. `JOB_FINISHED` no pertenece a un intento. Faltan eventos para `SKIPPED`.

**Decisión:** `Event` lleva `job_id`, `task` (string|null) y `type` incluye `TASK_SKIPPED`. `JOB_FINISHED` usa `execution_id=""`, `task=null`. Nuevo `kind="finished"` en `_METHODS`/`_CALLS` mapeado a `Job.finished()`, llamado por el coordinador cuando `job.state ∈ _FINISHED` tras `_settle`; se diariza y rejuega en el reinicio.

**Al implementar (2026-11-20):**
- El `job_id` de cada evento lo pone `Job`, no el llamante: el pseudocódigo de
  D15.6 lo pedía como parámetro y era repetir un dato que la máquina de estados
  ya tiene. Mismo contrato, menos superficie para equivocarse.
- `Job.finished()` es idempotente (`done`), y el coordinador pregunta `job.done`
  antes de diarizar: el replay no puede añadir un segundo final.
- Ventana de caída cerrada: si el coordinador muere entre la fila `result` y la
  fila `finished`, al reiniciar el trabajo ya está terminado y nadie volvería a
  llamar a `Job.finished()`. El constructor recorre los trabajos terminados sin
  final y lo diariza entonces; es el único caso en que un `at` es la hora del
  reinicio, porque el original nunca se escribió.
- El método `events(job_id)` del actor se adelantó al Commit 5: sin un lector,
  el criterio «los eventos sobreviven al reinicio» no se puede probar. El
  Commit 6 solo añade el endpoint HTTP y el cliente de la pasarela.

---

### D15.18 — TASK_LAUNCHED en la decisión, no en el arranque real

**Contexto:** `_launch` decide; `running()` arranca de verdad tras `retry_in_s`. Un solo tipo pierde la distinción.

**Decisión:** `TASK_LAUNCHED` se emite en `_launch`/`start` (cuando la máquina de estados decide lanzar). El arranque real de un reintento no genera evento aparte (el `RETRY_SCHEDULED` ya dijo cuándo se esperaba). Así el feed muestra "programado → espera → éxito/fallo" sin duplicados.

---

### D15.10 — Módulos puros no registran logs (refuerzo D10.4)

**Contexto:** D10.4 establece que `spec`, `dag`, `failures`, `scheduler`, `messages` no escriben logs operativos. La Fase 4 añade `logstore` (no puro) y extiende `scheduler` con eventos (datos, no logs).

**Decisión:** Reafirmar D10.4. `scheduler.Job._record_event` emite **eventos de datos** (estructura `Event`), no llamadas a `logging`. La capa HTTP (`gateway.py`) y el coordinador (`coordinator.py`) son quienes logean eventos operativos si procede (p.ej., `job finished` ya se logea en D10.11).

**Consecuencia:** Los tests unitarios de `scheduler` no necesitan capturar logs; verifican la lista `_events` directamente.

---

### D15.11 — Nombre y encabezado del archivo de un intento

**Contexto:** D15.2 fija un archivo por intento y un encabezado que repite el
identificador de ejecución. El identificador (`job/task/round/attempt`, D07.5)
contiene barras: usarlo tal cual como nombre crearía subcarpetas dentro del
almacén y, si alguna vez lo construyera el cliente, permitiría salir de su
carpeta con `..`.

**Opciones consideradas:**
1. **Ruta anidada** `logs/{job}/{task}/{round}/{attempt}.log`, validando cada
   componente.
2. **Identificador escapado** (`/` por `_`), con riesgo de colisión entre
   identificadores distintos.
3. **Nombre = SHA-256 hexadecimal del identificador**; el encabezado guarda el
   identificador completo.

**Decisión:** Opción 3.

**Porqué:**
- El nombre no puede contener barras ni `..`: no hay forma de elegir la ruta
  desde el identificador.
- No hay colisiones razonables, y no hace falta validar el alfabeto del
  identificador.
- El encabezado de D15.2 deja de ser redundante: sirve para detectar un archivo
  mal nombrado, truncado o mezclado, y para saber de qué intento es sin fiarse
  del nombre.
- Para leer, el almacén calcula el mismo hash y el encabezado confirma que el
  archivo corresponde al intento pedido.

**Consecuencias:**
- El archivo se llama `<sha256>.log` dentro de `/var/lib/dpes/logs`; listar por
  nombre legible no es posible (para eso está el encabezado).
- Un archivo cuyo encabezado no coincide con el intento pedido se rechaza con
  `ValueError` en vez de servirse.
- **Sin cerrojos en el núcleo:** el bucle de un actor asíncrono no cambia de
  llamada salvo en un `await`; `append` y `read` no esperan nunca, así que cada
  uno corre entero y `LogDir` no toma cerrojos. El `tail` sí espera (`await
  asyncio.sleep`), de ahí que no bloquee los `append` de los ejecutores.

### D15.12 — El actor del almacén se reinicia

**Contexto:** el actor `LogStore` guarda su índice en memoria y su verdad en
disco. Ray ofrece `max_restarts` por actor; `BlobStore` (plan/11) usa el valor
por defecto, sin reinicios.

**Opciones consideradas:**
1. **Sin reinicios** (`max_restarts=0`, el valor por defecto): si el proceso del
   actor muere, el nombre queda libre y el cliente que ya tiene el *handle* deja
   de poder llamarlo hasta que la pasarela se reinicie.
2. **`max_restarts=-1`**: Ray vuelve a crear el actor en el mismo nodo y el
   *handle* con nombre sigue valiendo.
3. **Hacer que el cliente recree el actor** cuando una llamada falla.

**Decisión:** Opción 2 para `LogStore` (no se cambia `BlobStore`).

**Porqué:**
- El actor no tiene estado propio que perder: el constructor solo crea la
  carpeta y el índice se reconstruye del archivo la primera vez que se toca un
  intento (D15.2). Reiniciar no puede servir datos falsos.
- Un `append` que se pierda por el reinicio se repite: es idempotente por `seq`
  (D15.1) y el ejecutor ya reintenta la transmisión (Commit 3). Por eso el
  reinicio no rompe la garantía «lo transmitido sobrevive» (F5.4): como mucho
  retrasa un fragmento.
- La opción 3 reparte por todo cliente (ejecutor y pasarela) la lógica de
  recuperación, que es justo lo que no debe saber ninguno de los dos (D15.5).
- Si el nodo de control entero cae, el nombre se pierde igual y da lo mismo la
  opción; la durabilidad que se defiende es la del volumen, y esa sigue ahí.

**Consecuencias:**
- `max_restarts=-1` en el decorador de `LogStore`: un constructor que fallara
  siempre provocaría reintentos con espera creciente de Ray (hoy el constructor
  solo crea la carpeta).
- Un `tail_fragments` en curso termina con error cuando el actor muere; el
  cliente puede volver a pedirlo desde el último byte recibido, que es el mismo
  trato que un cierre por inactividad (D15.4).

### D15.14 — El final del log de un intento

**Contexto:** D15.1 numera los fragmentos desde 1 y el almacén rechaza un
fragmento vacío (D15.2). D15.4 cierra un `tail` solo tras `IDLE_S` (300 s) sin
datos nuevos. Sin una señal explícita, un `logs -f` sobre un intento ya terminado
se queda cinco minutos esperando una salida que no va a llegar, y la pasarela no
puede distinguir «no hay más por ahora» de «no habrá más».

**Opciones consideradas:**
1. **Nada:** el seguidor espera `IDLE_S`. Cero código; cinco minutos de espera
   al final de cada `logs -f` y ninguna forma de saber que el intento acabó.
2. **Fragmento centinela con bytes reservados** (por ejemplo
   `x00DPES-EOF`): se mezcla con la salida, y una tarea puede escribir esos
   mismos bytes, así que el lector no puede distinguirlos.
3. **Bandera en la llamada** (`append_fragment(..., eof=True)`): cambia el
   contrato de D15.1/D15.8 y el formato en disco para guardar un booleano en
   cada fragmento.
4. **Registro de fin con `seq = 0`:** D15.1 reserva del 1 en adelante, así que 0
   no puede ser un fragmento; el mismo registro `(seq, longitud)` con `(0, 0)`
   marca el final sin cambiar el formato ni añadir un campo.
5. **Estado solo en memoria del actor:** se pierde al reiniciarlo (D15.12), justo
   cuando el cliente sigue leyendo.

**Decisión:** Opción 4 — un registro `(0, 0)` al cerrar, con `LogDir.close`,
`LogStore.close_fragments` y `_StoreClient.close_fragments`.

**Porqué:**
- Reutiliza el formato de D15.2 tal cual: no cambia la versión del encabezado ni
  el tamaño de los registros ya escritos, y un archivo escrito antes de esta
  decisión se sigue leyendo (sin marca de fin).
- `seq = 0` no puede confundirse con salida, porque el almacén exige `seq >= 1`,
  y una longitud 0 no aporta bytes: el índice y los offsets en bytes de D15.11
  no cambian.
- Es durable: un actor reiniciado vuelve a ver el fin al escanear el archivo,
  que es lo que la opción 5 no consigue.
- El `tail` termina en cuanto ve el fin, así que `logs -f` no se queda colgado
  cinco minutos sobre un intento terminado; la garantía F5.4 no cambia, porque
  el fin no añade salida.

**Consecuencias:**
- `close` es idempotente: la segunda llamada devuelve `False`.
- Un `append` posterior a `close` es `ValueError`: no es un reintento tardío
  admisible, sino un error de programación (el emisor para antes de cerrar).
- Un registro de fin cortado por una caída no cuenta: al escanear se descarta el
  último registro incompleto (D15.2), el archivo se trunca y el intento queda sin
  fin; el `tail` vuelve a esperar `IDLE_S`.
- El emisor cierra siempre el log, incluso cuando el comando no imprimió nada:
  el fin no es un fragmento y no aparece en la salida, pero es lo que permite que
  `logs -f` termine en cuanto el intento acaba.
- **Límite:** si el almacén no responde tras los reintentos de D15.8, el emisor
  deja de enviar el resto del log de ese intento y no lo cierra; la tarea no falla
  por ello (un log no puede tumbar un trabajo) y el `tail` acaba por inactividad.

### D15.15 — Cómo la pasarela sirve el log de una tarea

**Contexto:** D15.4 fija las dos formas de pedir el log (por offset y en vivo)
sobre una ruta que nombra la **tarea**, pero el almacén indexa por **intento**
(`job/task/round/attempt`): alguien tiene que traducir uno en otro, y la
traducción tiene que decir qué pasa cuando la tarea no tiene intento. Además un
`logs -f` puede durar minutos: hay que decidir cuándo se considera que el cliente
se fue y hasta dónde llega esa limpieza, porque el generador remoto del almacén no
se puede cancelar.

**Qué intento se sirve.**
1. **El que la vista del trabajo da para esa tarea** (`tasks[task].execution_id`,
   D09.6): el que está en vuelo o el que la resolvió. Ninguna API nueva.
2. **El que nombre el cliente** (`?attempt=N` o el execution id en la ruta):
   permite leer un intento perdedor, a cambio de exponer la numeración interna y
   validar que ese intento pertenece a la tarea.
3. **Todos los intentos concatenados:** mezcla salidas de intentos distintos, que
   es justo lo que la correlación por execution id evita (F5.5).

**Decisión:** la opción 1, con **404** cuando la tarea no tiene intento
(`execution_id` nulo: no se lanzó o se omitió). Una conexión en vivo se queda con
el intento que resolvió al principio: si hay reintento, el log de ese intento
termina y el seguidor acaba; el cliente que quiera el intento nuevo repite la
petición. Queda escrito como límite, no como olvido.

**Tamaño del log.** `Content-Range` y el 416 necesitan el tamaño total y
`read_fragments` no lo da. Opciones: (a) responder `bytes a-b/*`, legal con el
total desconocido; (b) añadir `log_size` al almacén, que ya lleva la cuenta en
memoria (`_size`). **Decisión:** (b): tres líneas por capa, `Content-Range` exacto
y, de paso, el tamaño que el `-n` del CLI necesita para estimar por bytes (D15.7).

**Framing.** `Transfer-Encoding: chunked` está definido para HTTP/1.1 y la
pasarela hablaba HTTP/1.0 (el valor por omisión de `BaseHTTPRequestHandler`).
Opciones: (a) `protocol_version = "HTTP/1.1"` y `Connection: close` en todas las
respuestas; (b) dejar HTTP/1.0 y escribir el framing igualmente (los clientes de
Python lo aceptan, pero un intermediario no tiene por qué). **Decisión:** (a).
`Connection: close` porque el CLI abre una conexión por orden —no se gana nada con
mantenerla— y porque un rechazo que no lee el cuerpo (411, 413) dejaría esos bytes
en una conexión viva y el siguiente «request» sería basura.

**Desconexión.** Con un `for` sobre el `tail`, el handler solo se enteraría al
escribir el fragmento siguiente, y un intento callado puede pasar minutos sin
escribir: el hilo de la pasarela quedaría retenido hasta `IDLE_S` (300 s) por cada
cliente que se va, y un cliente que abre y cierra conexiones agota los hilos de
`ThreadingHTTPServer`. Opciones: (a) enterarse solo al escribir; (b) leer el
`tail` en un hilo propio y, mientras no hay fragmento, preguntar al socket
(`select` + `MSG_PEEK`: legible y sin bytes = el cliente cerró). **Decisión:**
(b), con `POLL_S = 0.5`.

**Porqué:**
- La resolución por la vista no añade contratos ni deja al cliente adivinar la
  numeración de intentos, y un 404 explícito distingue «esa tarea no existe» de
  «todavía no corrió», que es lo que un usuario necesita al depurar.
- `log_size` es la única forma de que `Content-Range` diga la verdad; el total es
  un dato que el almacén ya tiene y no cuesta ni una lectura de disco.
- Con `Connection: close` el framing es legal y no hay keep-alive que romper: la
  respuesta se delimita por el último chunk y por el cierre, sin estados raros.
- La opción (b) de la desconexión devuelve el hilo del handler en medio segundo
  sin depender de que el intento escriba; la limpieza del lado del almacén no
  puede ser inmediata (Ray no cancela un generador remoto) y no se promete.

**Consecuencias:**
- La pasarela gana `log_size` en `_Client`; el almacén, `LogStore.log_size`. El
  CLI podrá usar el tamaño para su `-n` (D15.7).
- `Range` se acepta como alternativa a `offset` (bonus de D15.4): `bytes=N-` y
  `bytes=N-M` responden 206 con `Content-Range`; mezclar `Range` con `offset`,
  `limit` o `follow` es 400; un rango que empieza en el final o después es 416 con
  `Content-Range: bytes */total`; un `Range` que no tiene esa forma (el sufijo
  `bytes=-500`, por ejemplo) se ignora y se sirve el cuerpo entero, que es lo que
  la RFC permite.
- Un `offset` o `limit` que no es un número entero no negativo es 400; un
  `follow` que no es `0` ni `1`, también.
- Queda un hilo `dpes-tail` por conexión en vivo mientras dura: es el precio de
  detectar la desconexión; lo que no queda es el hilo del handler ni la espera de
  cinco minutos.
- Si un cliente deja de leer sin cerrar, la escritura se bloquea y la cola entre
  ese hilo y el handler crece con la salida del intento. Ahora está acotada por
  el tope del intento (§7.6, D15.13): la salida que se sirve no pasa de lo que
  el intento guardó, y el comando que la produce muere en el tope del
  trabajador.
- Los bytes que se sirven son los fragmentos tal cual: un lector que decodifique
  UTF-8 debe hacerlo tras unir, porque un corte puede caer dentro de un carácter.
- `RAY_DEDUP_LOGS=0` sigue siendo asunto del clúster (D15.9, `compose.yaml`); la
  pasarela no filtra ni fusiona líneas.

---

## 3. Contratos entre componentes (resumen)

| Productor | Consumidor | Contrato | Notas |
|-----------|------------|----------|-------|
| Executor (hilo emisor) | LogStore (actor) | `append_fragment(eid, seq, bytes) -> bool` | Idempotente por seq; lanza `LogTruncated` si el intento está lleno (D15.13) |
| Executor (hilo emisor) | LogStore (actor) | `close_fragments(eid) -> bool` | Marca el fin (D15.14) |
| LogStore (actor) | Coordinador (actor) | `drop(execution_ids) -> int` | Borra intentos superados; devuelve los bytes liberados (D15.13) |
| Ejecutor | `sandbox.run` | `capture_limit=None -> Outcome.overflow` | Mata el comando que pasa del tope (D15.13) |
| Ejecutor | Coordinador (actor) | `TaskResult.log_truncated: bool = False` | Campo aditivo; viaja en el resultado (D15.13) |
| LogStore (actor) | Gateway | `read_fragments(eid, offset, limit) -> [(seq, bytes)]` | Offset en bytes |
| LogStore (actor) | Gateway | `log_size(eid) -> int` | Bytes de salida (D15.15) |
| LogStore (actor) | Gateway | `tail_fragments(eid, offset) -> AsyncIterator[(seq, bytes)]` | Para streaming |
| Scheduler (núcleo) | Coordinador (actor) | `job.events() -> List[Event]` | Datos puros, sin Ray; reloj inyectado (D15.16) |
| Coordinador (actor) | Gateway | `events(job_id) -> list[dict] | None` | `None` si el trabajo no existe; `timestamp` flotante |
| Coordinador (actor) | `_Client` | `events(job_id)`, igual que `status(job_id)` | Bloqueantes, con `CALL_TIMEOUT_S` |
| Coordinador (actor) | Journal (SQLite) | `append(job, kind, *args) -> at` / `events() -> (job, kind, args, at)` | La hora de cada llamada (D15.16) |
| Gateway | Cliente CLI | HTTP `GET /logs?offset=&limit=` / `?follow=1` | text/plain, chunked |
| Gateway | Cliente CLI | HTTP `GET /jobs/{id}/events` | application/json; hora ISO 8601 UTC (D15.5) |

---

## 4. Pruebas de aceptación del corte C4 (para documentar en este plan)

| Test | Qué verifica | Nivel |
|------|--------------|-------|
| `test_following_prints_while_the_task_still_runs` | `logs -f` imprime salida antes de que la tarea termine | E2E |
| `test_logs_survive_worker_death` | `logs -o 0` devuelve fragmentos tras `docker kill worker` | E2E |
| `test_events_shows_history` | `GET /events` devuelve transiciones, clases, esperas | E2E |
| `test_the_offset_resumes_at_that_byte` | `logs -o N` retoma exactamente en byte N | E2E |
| `test_duplicate_fragment_ignored` | LogStore ignora seq duplicado | Unit |
| `EventsTest` | Cada transición genera el Event correcto (Commit 5) | Unit |
| `EventsApiTest` | `GET /jobs/{id}/events`: orden, claves, hora ISO, 404 (Commit 6) | Unit |
| `LogsTest` (test_cli) | `logs`: banderas, offset, `-n`, `-f` y códigos 0/1/2/3 (Commit 7) | Unit |
| `test_gateway_streaming_cleanup` | Desconexión cliente limpia suscripción tail | Integration |

---

## 5. Actualización de trazabilidad (plan/00-contexto-y-restricciones.md)

Hecha al cerrar la fase: tabla §2 actualizada:

| ID | Funcionalidad | Fases | Estado |
|----|---------------|-------|--------|
| R5 | Registro y transmisión de logs | 1, 4 | ✅ cumplido (C4, 2026-11-20) |

Decisiones D15.1–D15.18 registradas como cerradas en este documento; D15.13 se
decidió el 2026-11-21 en §7.6, después del corte C4.

---

## 6. Estado de la implementación

### Commit 1 — decisiones y contratos (2026-11-18)

`plan/15`. Sin código.

### Commit 2 — el actor `LogStore` y su núcleo en disco (2026-11-18)

**Qué hay:**

- `dpes/logstore.py`: `LogDir` (puro, sin Ray) y `tail` (generador asíncrono).
  Un archivo por intento, append-only, con encabezado que repite el
  identificador y registros `(seq, longitud, datos)` en little-endian (D15.2,
  D15.11). El índice `(seq, desplazamiento en la salida, longitud)` se
  reconstruye escaneando el archivo al abrirlo, y una escritura interrumpida al
  final se trunca (D15.2). Una escritura corta (un disco lleno puede aceptar
  menos bytes sin fallar) se detecta, se deshace en el archivo y se propaga como
  `OSError`, para que el índice nunca afirme más que el archivo.
- `dpes/coordinator.py`: actor `LogStore` (asíncrono, `num_cpus=0`,
  `max_restarts=-1` según D15.12) con `append_fragment`, `read_fragments` y
  `tail_fragments`; se crea en `connect()` con `lifetime="detached"` y afinidad
  de nodo, como `BlobStore`, sobre `/var/lib/dpes/logs` (el mismo volumen,
  D15.2). `_Client` gana `read_fragments` y `tail_fragments`; el segundo conduce
  el `ObjectRefGenerator` de Ray por dentro, así que la pasarela sigue sin saber
  de Ray (D15.5).

**Pruebas:**

| Plan (§4) | Test que lo evidencia | Nivel |
|-----------|-----------------------|-------|
| `test_duplicate_fragment_ignored` | `test_a_duplicate_seq_is_ignored`, `test_a_seq_that_was_already_passed_is_ignored` | Unit |
| Resumen por byte exacto | `test_a_byte_offset_slices_the_fragment_it_falls_in`, `test_a_limit_stops_mid_fragment` | Unit |
| Persistencia del almacén | `test_fragments_survive_reopening`, `test_a_reopened_store_keeps_numbering_where_it_stopped`, `test_a_half_written_fragment_is_dropped_on_reopening`, `test_a_creation_cut_short_starts_over`, `test_a_header_cut_inside_the_execution_id_starts_over` | Unit |
| Encabezado (D15.11) | `test_a_file_headed_by_another_attempt_is_refused`, `test_a_foreign_file_is_refused` | Unit |
| Escritura corta, como la de un disco lleno | `test_a_short_write_is_an_error_and_leaves_no_trace` | Unit |
| Cola viva | `test_tail_yields_what_arrives_while_it_waits`, `test_tail_ends_after_an_idle_stretch` | Unit |
| El actor en el clúster | `tests/integration/test_logstore.py`: escritura, seq duplicado, cola en vivo y otro actor sobre la misma carpeta | Integration |
| Reinicio del actor (D15.12) | `test_the_store_comes_back_after_its_process_is_killed` | Integration |

**Verificación (2026-11-18, clúster de tres nodos):**

- `py -W error -m unittest discover -s tests/unit` → 201 pruebas OK (2 saltadas
  por ser de Linux).
- En el nodo de control, `python -W error -m unittest discover -s tests/unit` →
  241 pruebas OK (sin saltos: el espacio aislado corre como `root`).
- `tests/integration` → 26 pruebas OK; `tests/e2e` → 23 pruebas OK.
- `py -m unittest discover -s tests/host` → 8 pruebas OK.
- Comprobación adicional fuera de las pruebas: 59 fragmentos de bytes
  aleatorios, lectura desde los 1154 desplazamientos posibles y 300
  `(offset, limit)` aleatorios contra la concatenación ingenua, y lo mismo tras
  reabrir el almacén; todo coincide.

**Límite conocido del `tail`:** termina tras `IDLE_S` (300 s) sin datos nuevos; el
cliente que quiera seguir pide otra cola desde el último byte (D15.4). Ray no
permite cancelar el generador remoto al cerrar el del llamante, de ahí el tope.

### Commit 3 — el ejecutor emite lo que el comando imprime (2026-11-19)

**Qué hay:**

- `dpes/sandbox.py`: `run(..., sink=None)` usa el archivo que le da el llamante en
  vez de crear siempre el suyo, y no lo cierra; la cola de 64 KiB (D07.14) se lee
  por el mismo descriptor.
- `dpes/executor.py`: `_Emitter`, un hilo *daemon* por intento que sigue ese
  archivo desde el principio y envía fragmentos por
  `store.append_fragment(eid, seq, data)` (D15.3, D15.8). Un fragmento acaba en
  un salto de línea cuando hay uno dentro del tope, y si no en `FRAGMENT_BYTES =
  4096`; también se corta a las `FRAGMENT_LINES = 100` líneas, o cuando pasan
  `FLUSH_S = 0.5` s desde el último envío para que una salida a cuentagotas no
  espere a llenar un fragmento. La `seq` empieza en 1 y solo avanza.
  Al terminar el comando se envía lo que quede y `close_fragments` marca el fin
  (D15.14), **siempre**, incluso si el comando no imprimió nada.
- Mejor esfuerzo, por diseño: si el almacén rechaza un fragmento tras los tres
  reintentos de `_SEND_WAITS`, se abandona el resto del log de ese intento con un
  `WARNING` que lleva `execution=`, `sent=` y `lost=`; la tarea no falla. Cuando
  todo llega, la única línea es `log closed` con `fragments=` y `bytes=` (D10.5).
- `dpes/coordinator.py`: `_StoreClient(actor, logs)` añade `append_fragment` y
  `close_fragments`; `_attempt` recibe el actor de logs y el `Coordinator` lo
  guarda y lo pasa a cada intento. El ejecutor sigue sin conocer Ray (D07.17).

**Pruebas:**

| Lo que se afirma | Test que lo evidencia | Nivel |
|------------------|-----------------------|-------|
| La salida llega numerada y completa | `test_the_output_arrives_in_fragments_with_consecutive_seq` | Unit |
| Fragmentos de líneas enteras, con tope de bytes | `test_a_long_output_comes_in_fragments_of_whole_lines` | Unit |
| El corte por tiempo no espera a llenar el fragmento | `test_output_that_trickles_out_is_sent_without_a_full_fragment` | Unit |
| Un comando mudo no produce fragmentos y aun así cierra el log | `test_the_log_is_ended_with_the_attempt_even_when_it_said_nothing` | Unit |
| Un envío fallido se reintenta con la misma `seq` | `test_a_fragment_is_retried_under_the_same_seq` | Unit |
| Un almacén que rechaza todo no hace fallar la tarea | `test_a_store_that_refuses_fragments_does_not_fail_the_task` | Unit |
| Un almacén que muere a mitad abandona el resto de ese log sin cerrarlo | `test_a_store_that_dies_midway_abandons_the_rest_of_the_log` | Unit |
| El ejecutor y el actor reales, juntos | `test_the_output_of_an_attempt_reaches_the_store` | Integration |
| Una cola termina en cuanto acaba el intento (D15.14) | `test_a_tail_ends_as_soon_as_the_attempt_does` | Integration |
| Reapertura, fin persistido y registro de fin truncado | `test_the_end_survives_reopening`, `test_an_end_marker_cut_short_does_not_count`, `test_a_log_closed_without_output_keeps_its_header` | Unit |

**Verificación (2026-11-19, clúster de tres nodos):**

- `py -W error -m unittest discover -s tests/unit` → 210 pruebas OK (2 saltadas
  por ser de Linux).
- En el nodo de control, `python -W error -m unittest discover -s tests/unit` →
  258 pruebas OK (sin saltos).
- `tests/integration` → 28 pruebas OK; `tests/e2e` → 23 pruebas OK.
- `py -m unittest discover -s tests/host` → 8 pruebas OK.
- Comprobación fuera de las pruebas, con el coordinador real: una tarea que
  imprime 2000 líneas deja 7 fragmentos, `seq` 1..7 consecutivas, 18893 bytes que
  coinciden byte a byte con la salida, el mayor de 4095 bytes; `tail` desde 0
  termina en 0,03 s en vez de esperar 300 s, y el archivo en el volumen acaba en
  el registro `(0, 0)`.

**Siguiente:** Commit 4 (la pasarela sirve los logs por offset y en vivo).

### Commit 4 — la pasarela sirve el log de una tarea (2026-11-19)

**Qué hay:**

- `dpes/logstore.py` y `dpes/coordinator.py`: `LogDir.size`, `LogStore.log_size`
  y `_Client.log_size` dan el total en bytes que `Content-Range` necesita
  (D15.15). No lee el disco: usa el `_size` que el índice ya lleva.
- `dpes/gateway.py`: ruta `_LOG_PATH` (`/jobs/{job}/tasks/{task}/logs`), probada
  antes que la del estado para que `.../logs` no se confunda con un identificador
  de trabajo. `_answer_logs` pasa de `{job, task}` al intento que la vista da
  (`tasks[task].execution_id`, D15.15) y responde 404 si no existen el trabajo,
  la tarea o el intento; `_log_request` valida `offset`, `limit`, `follow` y
  `Range` (400 cuando no cuadran). `Range` se sirve como 206 con
  `Content-Range`, como 416 con `bytes */total` si empieza en el final o después,
  y se ignora (200 con el cuerpo entero) cuando no es un rango de bytes que este
  servidor sirva.
- `_follow` responde `text/plain` con `Transfer-Encoding: chunked`; la pasarela
  habla HTTP/1.1 y cierra todas las respuestas (`Connection: close`, D15.15).
  Escribe un chunk por fragmento y termina con el chunk vacío cuando el intento
  cerró su log (D15.14). El `tail` se lee en un hilo `dpes-tail` propio
  (`_Reader`), y el handler pregunta cada `FOLLOW_POLL_S = 0,5` s si su cliente
  sigue ahí (`select` + `MSG_PEEK`): una conexión abandonada se suelta en medio
  segundo y no en los 300 s de `IDLE_S`.
- Registro (D10.5): `logs served` (`INFO`) con `execution=`, `offset=`, `bytes=`
  y, si lo hay, `limit=`; `log followed` (`INFO`) o `log cut off` (`WARNING`)
  con `execution=`, `offset=`, `bytes=` y `stopped=` (`end`, `client`, `error`);
  `log cut short` (`WARNING`) si el cliente se va con el cuerpo a medio
  escribir. La pasarela no reescribe la salida (D15.9, `RAY_DEDUP_LOGS=0` sigue
  siendo asunto del clúster).

**Pruebas:**

| Lo que se afirma | Test que lo evidencia | Nivel |
|------------------|-----------------------|-------|
| El intento se sirve entero y en `text/plain` | `test_the_output_of_the_task_attempt_is_served_whole` | Unit |
| `offset` y `limit` cortan por bytes | `test_offset_and_limit_slice_the_output_in_bytes`, `test_an_output_past_its_end_is_an_empty_answer` | Unit |
| Trabajo, tarea o intento desconocidos: 404 | `test_an_unknown_job_task_or_attempt_is_not_found` | Unit |
| Parámetros que no son números enteros: 400 | `test_a_parameter_that_is_not_a_whole_number_is_refused` | Unit |
| `Range` como alternativa a `offset` (206 y `Content-Range`) | `test_a_range_header_takes_over_the_offset`, `test_a_range_with_an_end_limits_the_answer` | Unit |
| Un `Range` que no se sirve se ignora; uno imposible es 416 | `test_a_range_this_server_does_not_serve_is_ignored`, `test_a_range_beyond_the_end_is_not_satisfiable` | Unit |
| `Range` mezclado con la consulta es 400 | `test_a_range_mixed_with_the_parameters_is_refused` | Unit |
| El tamaño del log, sin leer el disco (D15.15) | `test_the_size_is_the_output_stored`, `test_an_attempt_that_wrote_nothing_has_no_size` | Unit |
| `follow` transmite fragmento a fragmento y en chunks | `test_following_streams_the_output_as_it_comes`, `test_the_follow_body_is_framed_as_chunks` | Unit |
| El fin del intento cierra el flujo | `test_a_follow_that_reaches_the_end_says_so` | Unit |
| Un cliente que se va deja de seguirse | `test_a_client_that_leaves_is_not_followed_any_more` | Unit |
| Un cliente que se va con el cuerpo a medio escribir no es un fallo | `test_a_client_that_leaves_before_its_body_is_out_is_logged` | Unit |
| Un almacén que falla corta el flujo, no lo termina | `test_a_tail_that_fails_cuts_the_stream_off` | Unit |
| Cada respuesta deja su traza con `execution=` | `test_serving_a_log_is_logged_with_its_execution` | Unit |
| El camino completo, de la pasarela al actor real | `tests/integration/test_logstore.py`: `test_the_gateway_serves_the_bytes_the_attempt_wrote`, `test_a_follower_sees_the_output_as_the_attempt_writes_it`, `test_a_client_that_leaves_is_not_followed_any_more` | Integration |

**Verificación (2026-11-19, clúster de tres nodos):**

- `py -W error -m unittest discover -s tests/unit` → 229 pruebas OK (2 saltadas
  por ser de Linux).
- En el nodo de control, `python -W error -m unittest discover -s tests/unit` →
  277 pruebas OK (sin saltos).
- `tests/integration` → 31 pruebas OK; `tests/e2e` → 23 pruebas OK.
- `py -m unittest discover -s tests/host` → 8 pruebas OK.
- Comprobación fuera de las pruebas, con la pasarela del nodo de control y un
  trabajo que imprime tres líneas espaciadas: `follow=1` las emitió según se
  producían y cerró al terminar la tarea; `offset=6&limit=6` devolvió `line2n`;
  `Range: bytes=12-` → 206 con `Content-Range: bytes 12-17/18`;
  `Range: bytes=18-` → 416 con `bytes */18`; una tarea desconocida → 404;
  `offset=x` → 400; `Range` junto a `offset` → 400.

**Siguiente:** Commit 5 (la emisión de eventos del scheduler, D15.6, D15.16 y D15.17).

---

### Commit 5 — el scheduler emite eventos y el diario guarda su hora (2026-11-20)

**Qué hay:**

- `dpes/scheduler.py`: `Event` (inmutable) con `timestamp`, `type`, `job_id`,
  `task`, `execution_id` y `payload`; `Job._events`, `Job.events()`, `Job.done`,
  `Job.finished()` y `_record_event(type, execution_id, task, **payload)`. El
  módulo sigue sin `import time`: el reloj entra por `clock=` (D15.16) y sin él
  los eventos se sellan en `0.0`.
- Puntos de emisión (D15.5, D15.18): `_launch` → `TASK_LAUNCHED` (primer intento
  y los que desbloquea una dependencia); `on_result` con éxito →
  `ATTEMPT_SUCCEEDED` (`state`, `exit_code`, `note` si la hubo); `_failed` →
  `ATTEMPT_FAILED` siempre (`state`, `failure_class`, `reason`, `retry_in_s`) y,
  si queda presupuesto, `RETRY_SCHEDULED` (`failure_class`, `retry_in_s`,
  `next_execution_id`), las dos con el intento que falló para poder enlazarlas;
  `_skip_dependents_of` → `TASK_SKIPPED` por tarea omitida (`execution_id=""`);
  `finished()` → `JOB_FINISHED` una sola vez (`task=None`, `execution_id=""`,
  `counts` por estado de tarea). `running()` no emite nada: el reintento ya se
  anunció al programarse.
- `dpes/store.py`: `Journal` gana `at REAL NOT NULL DEFAULT 0`; `append`
  devuelve la hora que guarda y `events()` devuelve
  `(job, kind, args, at)`. La migración es idempotente y se decide con
  `PRAGMA table_info(events)` (D15.16).
- `dpes/coordinator.py`: `_Clock` es el reloj de pared que el `Job` recibe;
  mientras corre una llamada diarizada devuelve el `at` de esa fila
  (`_journaled`), tanto en vivo como en el replay, así que el evento
  reconstruido es idéntico al original. `_METHODS`/`_CALLS` ganan `finished`.
  `_log_if_finished` diariza el final antes de registrarlo, y el constructor
  cierra los trabajos que el replay deja terminados sin fila `finished`
  (D15.17). El actor expone `events(job_id)`, que devuelve `None` si el trabajo
  no existe.

**Pruebas:**

| Lo que se afirma | Test que lo evidencia | Nivel |
|------------------|-----------------------|-------|
| Cada transición deja su evento con el esquema completo | `EventsTest.test_starting_a_task_leaves_one_launch_event`, `test_a_success_records_the_attempt_and_its_exit_code`, `test_a_worker_failure_keeps_its_class_and_reason` | Unit |
| El reloj inyectado sella los eventos | `test_events_follow_the_injected_clock` | Unit |
| Un resultado adoptado se explica en el evento | `test_an_adopted_result_says_so` | Unit |
| Un fallo de tarea sin presupuesto es un solo `ATTEMPT_FAILED` | `test_a_task_failure_out_of_budget_is_one_failed_event` | Unit |
| Un reintento es `ATTEMPT_FAILED` + `RETRY_SCHEDULED` enlazados | `test_a_retry_is_a_failure_then_a_scheduled_retry` | Unit |
| Las tareas omitidas dejan `TASK_SKIPPED` sin intento | `test_skipping_a_dependent_leaves_an_event_without_an_attempt` | Unit |
| `JOB_FINISHED` aparece una vez, con recuentos | `test_the_end_of_a_job_is_recorded_once_with_the_task_counts` | Unit |
| Repetir las llamadas con las mismas horas reconstruye el mismo feed | `test_replaying_the_same_calls_rebuilds_the_same_feed` | Unit |
| El feed es JSON puro con las claves del esquema | `test_the_feed_is_plain_json` | Unit |
| El diario devuelve cada llamada con su hora, también al reabrir | `JournalTest.test_each_call_keeps_the_wall_clock_time_it_was_made`, `test_that_time_survives_reopening` | Unit |
| Un diario anterior a D15.16 se migra sin perder llamadas | `JournalMigrationTest.test_rows_without_a_time_gain_the_column_and_keep_their_data`, `test_new_rows_of_a_migrated_journal_keep_their_time` | Unit |
| Un trabajo desconocido no tiene feed | `CoordinatorTest.test_an_unknown_job_has_no_events` | Integration |
| El feed real enseña fallo, espera y reintento | `RetryTest.test_the_feed_shows_the_failure_the_retry_and_the_end` | Integration |
| El feed de un trabajo terminado sobrevive al reinicio con sus horas | `RecoveryTest.test_the_feed_of_a_finished_job_survives_unchanged` | Integration |
| Una caída entre `result` y `finished` no deja el feed sin final | `ReplayedJournalTest.test_a_job_that_finished_just_before_a_crash_records_its_end` | Integration |

**Verificación (2026-11-20, clúster de tres nodos):**

- `py -W error -m unittest discover -s tests/unit` → 244 pruebas OK (2 saltadas
  por ser de Linux).
- En el nodo de control, `python -W error -m unittest discover -s tests/unit` →
  292 pruebas OK (sin saltos).
- `tests/integration` → 35 pruebas OK; `tests/e2e` → 23 pruebas OK.
- `py -m unittest discover -s tests/host` → 8 pruebas OK. En dos pasadas
  inmediatas a un `up -d --build` falló `test_every_process_describes_its_part_of_the_job`:
  Ray reenvía la salida de los workers de forma asíncrona y la prueba deja de
  esperar en cuanto ve `job finished` (línea del coordinador), antes de que
  lleguen las del ejecutor del primer intento. Es una carrera conocida de esa
  prueba del espacio anfitrión (pista B), no de este cambio: sin tocar código,
  la pasada siguiente volvió a verde.
- Migración sobre el volumen real: `PRAGMA table_info(events)` del diario que ya
  existía en `/var/lib/dpes` lista `at`; las 930 filas anteriores quedaron en
  `0.0` y las nuevas traen hora.
- Comprobación fuera de las pruebas: un DAG de tres tareas produjo siete
  eventos en orden (`TASK_LAUNCHED` ×3, `ATTEMPT_SUCCEEDED` ×3 y
  `JOB_FINISHED` con `counts: {"succeeded": 3}`); se mató el coordinador y el
  feed volvió **idéntico**, con los mismos flotantes.

**Siguiente:** Commit 6 (el endpoint `GET /jobs/{id}/events`), que solo añade el
transporte HTTP y `_Client.events`.

---

### Commit 6 — la pasarela sirve el feed de eventos (2026-11-20)

**Qué hay:**

- `dpes/gateway.py`: ruta `_EVENTS_PATH` (`/jobs/{job}/events`), probada antes
  que la del estado y con `unquote`, como la de logs. `_serve_events` llama a
  `coordinator.events(job_id)`: 404 si el trabajo no existe, `[]` si existe y
  aún no tiene eventos, 200 con la lista en el orden del actor (orden de
  inserción, D15.16). 503 si el coordinador no contesta, como el resto.
- El instante pasa a texto en el transporte: `_wire` copia el evento y cambia su
  `timestamp` por el de `_iso` (ISO 8601 UTC con milisegundos, D15.5). El actor
  sigue devolviendo el flotante del diario para que el replay sea idéntico.
- `dpes/coordinator.py`: `_Client.events(job_id)`, una llamada bloqueante más
  con `CALL_TIMEOUT_S`, igual que `status`.
- La consulta se registra en DEBUG (`events served`, `job`, `found`, `events`):
  es un endpoint que los clientes sondean, y nunca se registra su contenido
  (D10.5, D10.6).

**Pruebas:**

| Lo que se afirma | Test que lo evidencia | Nivel |
|------------------|-----------------------|-------|
| El feed sale en el orden en que ocurrió, y se pide el trabajo de la ruta | `EventsApiTest.test_the_feed_is_served_oldest_first` | Unit |
| Cada evento lleva `job_id`, `task`, `execution_id` y su `payload` | `test_every_event_carries_the_keys_that_correlate_it` | Unit |
| `JOB_FINISHED` no pertenece a ninguna tarea | `test_the_end_of_the_job_belongs_to_no_task` | Unit |
| La hora es ISO 8601 UTC con milisegundos, con acarreo correcto | `test_the_time_is_iso_8601_utc_with_milliseconds` | Unit |
| Un trabajo sin eventos es `[]`; uno desconocido, 404 | `test_a_job_with_no_events_yet_is_an_empty_feed`, `test_an_unknown_job_is_not_found` | Unit |
| Un coordinador caído es 503, no un cuerpo vacío | `test_an_unreachable_coordinator_is_reported_as_unavailable` | Unit |
| Sondear el feed no escribe líneas INFO | `test_polling_the_feed_stays_below_info` | Unit |
| `_Client.events` trae el feed real de un trabajo con reintento | `RetryTest.test_the_feed_shows_the_failure_the_retry_and_the_end` | Integration |
| El feed sobrevive al reinicio del coordinador, horas incluidas | `RecoveryTest.test_the_feed_of_a_finished_job_survives_unchanged` | Integration |

**Verificación (2026-11-20, clúster de tres nodos):**

- `py -W error -m unittest discover -s tests/unit` → 252 pruebas OK (2 saltadas).
- En el nodo de control, `python -W error -m unittest discover -s tests/unit` →
  300 pruebas OK; `tests/integration` → 35 OK; `tests/e2e` → 23 OK;
  `tests/host` → 8 OK. Una pasada de las unitarias, inmediatamente después de
  reconstruir la imagen, terminó en `FAILED (failures=1)` sin que se capturara
  el nombre; las cinco siguientes (tras reiniciar los contenedores y tras otra
  reconstrucción) dieron 300 OK, así que se anota como fallo no reproducido de
  las pruebas que dependen de tiempos en un clúster recién levantado, no como
  algo de este cambio. **Diagnosticado en el Commit 7:** era una carrera de las
  pruebas con el orden en que la pasarela registraba lo que había servido (ver
  Commit 7, «el orden de los registros»), no un fallo de los eventos.
- Comprobación fuera de las pruebas, contra la pasarela real: un trabajo de dos
  tareas devolvió por HTTP cinco eventos en orden, con horas como
  `2026-11-20T03:47:21.963Z`, `task: null` en `JOB_FINISHED` y
  `counts: {"succeeded": 2}`; `/jobs/nope/events` respondió 404. Se mató el
  coordinador y la misma petición devolvió el **mismo JSON**, byte a byte.

**Siguiente:** Commit 7 (el comando `logs` del CLI), que consumirá este endpoint.

---

### Commit 7 — el comando `logs` del CLI (2026-11-20)

**Qué hay:**

- `dpes/cli.py`: subcomando `logs <job> <task> [-f] [-o N] [-n N]`. Sin `-f`
  pide `GET /jobs/{job}/tasks/{task}/logs?offset=N` entero y se queda con las
  últimas `-n` líneas; con `-f` pide `?follow=1&offset=N` y va escribiendo lo que
  llega. Solo lectura: no manda `Idempotency-Key`.
- **Códigos de salida (D15.7):** 0 si el log se sirvió o el seguimiento
  acabó (fin del intento o Ctrl+C); 1 si la pasarela contestó 4xx/5xx que no es
  404; 2 si es 404 (trabajo, tarea o intento inexistente); 3 si no hubo
  respuesta. La distinción se calcula en un solo sitio (`_failure_code`) y solo
  para `logs`: `status`, `artifacts` y `submit` siguen devolviendo 3 para todo
  (deuda declarada en D15.7). Para poder mirar el código, `RequestFailed` pasa a
  llevar `status`, y `_request` se parte en `_request`/`_open` porque el
  seguimiento necesita la respuesta abierta, no el cuerpo leído.
- El seguimiento lee con `read1` (devuelve en cuanto hay bytes; un `read`
  esperaría a llenar el búfer y dejaría de ser en vivo) y decodifica con un
  decodificador incremental de UTF-8, porque un corte de fragmento puede caer
  dentro de un carácter (D15.4). No pone plazo propio: la pasarela cierra el
  flujo cuando el intento cierra su log. Ctrl+C sale con 0.
- `-n` es **exacto**, no una estimación por bytes: se pide desde `-o` y se
  recortan las últimas N líneas. `-n 0` imprime todas. `-n` con `-f` y un `-n`
  negativo son error de uso (siguen saliendo con 2, como todo `argparse`).
- **Corrección en `dpes/gateway.py` (Commit 4):** `_serve_logs` trataba
  **cualquier** `OSError` como «el cliente se fue», así que un fallo del
  coordinador en esa ruta se quedaba sin respuesta (conexión cerrada) en vez de
  un 503. Ahora solo `BrokenPipeError` y `ConnectionResetError` cuentan como
  cliente que se fue; lo demás llega como 503, que el CLI convierte en su 1.
- **Y el orden de los registros de la pasarela (Commit 4):** «logs served» y
  «log followed» se escribían **después** de mandar el cuerpo, así que un
  cliente que ya tenía la respuesta podía mirar los registros y no estar aún
  el suyo. Eso hacía intermitentes las pruebas unitarias del Commit 4 (una vez
  faltaba el registro, otra aparecía en la captura de la prueba siguiente), y es
  el `FAILED (failures=1)` que quedó sin nombre en la verificación del Commit 6
  (§ arriba): no era de aquel cambio. Ahora la pasarela registra **antes** de
  mandar el último byte (y, en el seguimiento, antes del trozo final, con el
  seguimiento registrado una sola vez, `_note_follow`); un fallo al escribir ese
  trozo ya no se registra dos veces. Con eso, 40 pasadas seguidas de las
  unitarias en Windows salen en verde, frente a una de cada seis antes.
- `tests/unit/test_cli.py` espera el registro del seguimiento antes de volver
  (`follow_cli`), y `tests/unit/test_gateway.py` hace lo mismo en las pruebas de
  seguimiento: la ordenación de arriba ya basta, pero la espera documenta la
  propiedad y protege del caso en que el cliente se va.
- `tests/e2e/gateway_client.cli` acepta `url=` para probar «no hay pasarela» sin
  salir del contenedor.

**Pruebas:**

| Lo que se afirma | Test que lo evidencia | Nivel |
|------------------|-----------------------|-------|
| El log entero sale y el comando acaba en 0 | `LogsTest.test_the_whole_log_is_printed_and_it_ends_well` | Unit |
| `-o N` pide desde el byte N | `test_the_offset_asks_for_the_log_from_that_byte` | Unit |
| `-n` se queda con las últimas líneas (`0`: todas; de más: todas) | `test_lines_keeps_the_end_of_the_log` | Unit |
| `-n` cuenta desde el `-o` | `test_lines_counts_from_the_offset` | Unit |
| `-f` imprime lo que llega, y no hace petición de lectura entera | `test_following_prints_the_log_as_it_comes` | Unit |
| `-f -o N` pide el seguimiento desde N | `test_following_from_an_offset_asks_from_there` | Unit |
| Un carácter partido entre fragmentos se imprime entero | `test_a_character_split_between_fragments_is_printed_whole` | Unit |
| Ctrl+C corta el seguimiento con 0 | `test_ctrl_c_stops_a_follow_without_an_error` | Unit |
| Trabajo, tarea o intento inexistente → 2 (404) | `test_an_unknown_job_is_not_found`, `test_an_unknown_task_is_not_found`, `test_a_task_that_never_ran_is_not_found` | Unit |
| Un 4xx/5xx que no es 404 → 1 | `test_any_other_refusal_of_the_gateway_is_one` | Unit |
| Un coordinador que falla → 1 (503), no un corte | `test_a_coordinator_that_breaks_is_one` | Unit |
| Sin respuesta de la pasarela → 3 | `test_a_gateway_that_cannot_answer_at_all_is_three` | Unit |
| `-n` con `-f` y `-n` negativo son error de uso (2, sin petición) | `test_lines_and_follow_together_are_a_usage_error`, `test_a_negative_line_count_is_a_usage_error` | Unit |
| Un coordinador que falla en la ruta de logs es 503, no un corte | `LogApiTest.test_a_coordinator_that_breaks_is_unavailable_not_a_cut` | Unit |
| El registro de lo servido existe ya cuando el cliente ve el cuerpo | `LogApiTest.test_serving_a_log_is_logged_with_its_execution`, `test_a_follow_that_reaches_the_end_says_so` | Unit |
| El log de un intento acabado se sirve entero | `test_the_log_of_a_finished_attempt_is_served_whole` | E2E |
| `-o N` retoma exactamente en el byte N | `test_the_offset_resumes_at_that_byte` | E2E |
| `-n` recorta el final | `test_lines_keeps_the_end_of_the_log` | E2E |
| El log de una tarea fallida también se sirve | `test_the_log_of_a_failed_task_is_served_too` | E2E |
| `-f` imprime **antes** de que la tarea termine | `test_following_prints_while_the_task_still_runs` | E2E |
| Un trabajo o tarea inexistente → 2 (404) | `test_a_job_or_task_that_does_not_exist_is_not_found` | E2E |
| Una petición rechazada → 1 (400) | `test_a_refused_request_is_one` | E2E |
| Una pasarela que no está → 3 | `test_a_gateway_that_is_not_there_is_three` | E2E |

**Verificación (2026-11-20 → 2026-11-21, clúster de tres nodos):**

- `py -W error -m unittest discover -s tests/unit` → 273 pruebas OK (2
  saltadas) en Windows; **40 pasadas seguidas** en verde tras ordenar los
  registros de la pasarela (el `FAILED (failures=1)` sin nombre del Commit 6
  era una carrera de ese orden, corregida en C7).
- En el nodo de control, imagen reconstruida: `python -W error -m unittest
  discover -s tests/unit` → 321 OK; `tests/integration` → 35 OK;
  `tests/e2e` → 33 OK; `tests/host` → 9 OK.
- Cobertura (plan/15 D15.20, `coverage.py` solo de desarrollo/CI): `dpes/
  logstore.py` 100 %, `dpes/scheduler.py` 100 % (0 de 359 sentencias sin
  probar), umbral 90 % en CI.
- Comprobación manual fuera de las pruebas, contra la pasarela real, con un
  trabajo `echo one; echo two`: log entero → `onentwon` y 0; `-o 4` → `two`
  y 0; `-n 1` → `two` y 0; tarea inexistente → `HTTP 404: unknown task` y 2;
  `--url http://127.0.0.1:9` → `cannot reach the gateway` y 3; `-o -1` →
  `HTTP 400: offset: a whole number` y 1.

**Commits 8-10 (ampliación de pruebas y corte C4, 2026-11-21):**

- Commit 8, pruebas unitarias: `tests/unit/test_logstore.py` (tope de fragmento
  de 4 GiB, id de ejecución demasiado largo para la cabecera, lectura que se
  detiene al alcanzar el límite) y `tests/unit/test_scheduler.py` (fallo por
  salida declarada ausente). `dpes/logstore.py` y `dpes/scheduler.py` al 100 %
  de cobertura. CI mide con `coverage.py` (`requirements-dev.txt`,
  `docker/Dockerfile`, `.github/workflows/ci.yml`).
- Commit 9, pruebas de integración: ya existían `tests/integration/
  test_logstore.py` y `tests/integration/test_coordinator.py` (LogStore actor,
  emisión del ejecutor, feed que sobrevive al reinicio); pasan en el clúster
  (35 OK). Ninguna nueva necesaria para C4.
- Commit 10, extremo a extremo (corte C4): `tests/e2e/test_logs.py` añade
  `test_the_log_outlives_the_executor_that_wrote_it` (log servido tras
  `lose-ack`, R7) y `test_the_feed_tells_the_history_of_a_retried_task`
  (feed ordenado con `TASK_LAUNCHED`, `ATTEMPT_FAILED`, `RETRY_SCHEDULED`,
  `ATTEMPT_SUCCEEDED`, `JOB_FINISHED`, horas ISO 8601 UTC con ms, D15.5);
  `tests/e2e/gateway_client.py` expone `events()`; `tests/host/
  test_worker_loss.py` añade `test_the_log_of_an_attempt_survives_the_death_
  of_its_worker` (muerte real del contenedor, `docker kill`, y `live_nodes`
  parsea el entero de Ray de forma robusta). C4 verde (33 e2e + 9 host).

**Siguiente:** Commit 11 (diagramas y trazabilidad final, 2026-11-21): hecho.

### Commit 11 — Diagramas y trazabilidad final (2026-11-21)

**Qué hay:**

- `plan/diagramas/flujo-logs.mmd` + `flujo-logs.svg` (nuevo): flujo de logs,
  Cliente → Pasarela → LogStore ← Ejecutor (fragmentos) y Pasarela → Cliente
  (flujo en vivo).
- `plan/diagramas/dependencias-modulos.mmd` + `dependencias-modulos.svg`
  (actualizado): se añade el nodo `logstore.py` (clase E/S, disco), con aristas
  `executor → logstore` (fragmentos) y `gateway → logstore` (lectura).
- `plan/00-contexto-y-restricciones.md` §2: R5 pasa a
  `✅ cumplido (C4, 2026-11-20)`.
- `plan/15-fase-4-logs.md`: sección 6 cerrada con Commits 1–11 y sección 4
  con la tabla de aceptación de C4 completa (unit, integración, e2e, host).

**Verificación (D05.1):**

- `flujo-logs.svg` y `dependencias-modulos.svg` regeneradas desde sus `.mmd`
  con `npx @mermaid-js/mermaid-cli`; ambas abren y contienen los nodos y
  aristas declarados.
- Ningún marcador «planificado» queda en el diagrama de módulos: `logstore`
  aparece como nodo implementado (clase E/S) y no como `previsto`.

### Commit 12 — retención y contrapresión de los logs (2026-11-21)

Cierra el límite conocido de §7 con la decisión D15.13 (§7.6).

**Qué hay:**

- `dpes/logstore.py`: `MAX_ATTEMPT_BYTES` (100 MiB, `DPES_LOG_MAX_MIB`) como
  tope por intento; `LogDir(root, max_bytes=None)`; un fragmento que no cabe
  lanza `LogTruncated(execution_id, size, limit)` y no se escribe. La
  comprobación de duplicado (D15.1) va **antes** que la del tope, así que
  reintentar el `seq` que ya está no se confunde con una truncación. `drop(ids)`
  borra los archivos de los intentos que un trabajo terminado ya no sirve y
  devuelve los bytes liberados; los ids descartados se recuerdan en `_dropped`
  para que un fragmento tardío no recree el archivo y un seguidor suyo termine
  en el acto en vez de esperar `IDLE_S`.
- `dpes/coordinator.py`: `LogStore(root, max_bytes=None)` anuncia su tope al
  arrancar y convierte el `LogTruncated` en el evento de sistema `log truncated`
  con `side="control"`, `bytes` y `limit` (D10.5) antes de relanzarlo; gana
  `drop`. `_sweep(job)`, llamado desde `_log_if_finished` con `_spawn`, conserva
  el intento al que apunta la vista (`tasks[t]["execution_id"]`, el que resolvió
  la tarea o el último fallido) y borra los demás de `attempts`. Ni diario ni
  estado: es espacio, se puede repetir y es de mejor esfuerzo; si el actor no
  contesta se registra `log sweep failed` y el trabajo no cambia (D15.13).
- `dpes/sandbox.py`: `run(..., capture_limit=None)` y `Outcome.overflow`. `_wait`
  sondea cada 0.1 s y, al pasarse del tope, mata el grupo del comando y lo
  informa como desbordamiento, no como plazo. Truncar el archivo no libera el
  disco: el hijo escribe con el desplazamiento que comparte con el padre y la
  siguiente escritura lo vuelve a extender.
- `dpes/executor.py`: `MAX_CAPTURE_BYTES` (50 MiB, `DPES_LOG_CAPTURE_MIB`). El
  emisor corta el último fragmento a lo que cabe, deja de enviar al llegar y
  **cierra el log igual** (para que un `logs -f` termine, D15.4/D15.14); `_send`
  distingue `LogTruncated` (el intento está lleno, el almacén está vivo) de un
  fallo del almacén, que sigue tomándose por muerto. El evento `log truncated`
  lleva `side` (`control` o `worker`). `LogTruncated.__reduce__` la reconstruye
  con sus tres campos: Ray transporta la excepción pickleándola y el camino por
  omisión (solo el mensaje) la dejaba en `UnserializableException`, con lo que
  el emisor tomaba el almacén por muerto.
- `dpes/messages.py`: `TaskResult.log_truncated: bool = False`, campo aditivo
  (los constructores existentes no cambian); es el único cambio de contrato de
  este commit y queda registrado aquí.
- `dpes/scheduler.py`: `_Task.truncated`, expuesto como `truncated` en la vista
  de la tarea; el motivo del fallo es «output over the log limit» y el flag se
  vuelve a `False` cuando un intento posterior acaba sin resultado.
- `plan/00-contexto-y-restricciones.md` §6: el riesgo de crecimiento sin cota
  pasa a cerrado.

**Pruebas:**

| Qué | Test que lo evidencia | Nivel |
|-----|-----------------------|-------|
| Tope por intento y hueco de `seq` | `test_a_fragment_that_does_not_fit_is_refused`, `test_a_fragment_that_fills_the_ceiling_exactly_is_stored`, `test_a_refused_fragment_leaves_a_gap_the_next_one_can_use`, `test_the_ceiling_defaults_to_the_configured_one` | Unit |
| Un duplicado no es una truncación | `test_a_repeated_seq_is_not_taken_for_a_truncation` | Unit |
| La excepción cruza el actor | `test_truncation_survives_the_trip_through_ray`, `test_the_actor_raises_truncation_as_its_own_type` | Unit, Integration |
| Barrido y seguidor tardío | `test_a_dropped_attempt_is_forgotten_and_its_file_gone`, `test_dropping_twice_frees_nothing_the_second_time`, `test_dropping_an_attempt_that_never_wrote_frees_nothing`, `test_a_late_fragment_does_not_recreate_a_dropped_log`, `test_only_the_attempts_given_are_dropped`, `test_tail_of_a_dropped_attempt_ends_at_once` | Unit |
| El coordinador barre al cerrar | `LogRetentionTest` (`test_the_attempt_that_settled_the_task_keeps_its_log`, `test_a_superseded_attempt_loses_its_log_when_the_job_closes`) | Integration |
| Tope de captura del trabajador | `test_the_worker_ceiling_cuts_the_last_fragment_to_what_fits`, `test_a_flooding_command_is_killed_and_the_log_says_it_was_cut`, `test_a_command_that_floods_its_output_is_killed_at_the_limit`, `test_a_command_under_the_limit_runs_to_its_end` | Unit |
| Vista, motivo y trabajos normales | `test_a_cut_log_is_shown_in_the_view_but_does_not_fail_the_task`, `test_a_command_killed_for_its_log_fails_with_that_reason`, `test_a_task_that_ran_normally_is_not_marked_truncated` | Unit |
| Un trabajo que imprime sin pausa | `test_a_runaway_command_stops_at_the_log_limit` | E2E |

**Verificación (2026-11-21, clúster de tres nodos):**

- `py -W error -m unittest discover -s tests/unit` → 288 pruebas OK (2 saltadas
  por ser de Linux).
- En el nodo de control, las mismas con cobertura → 341 pruebas OK; cobertura de
  `dpes/executor.py` 95 %, `dpes/logstore.py` 99 %, `dpes/sandbox.py` 97 %,
  `dpes/scheduler.py` 100 %.
- `tests/integration` → 40 pruebas OK; `tests/e2e` → 34 pruebas OK (la del
  comando desbocado termina en segundos: el tope del trabajador mata el comando
  antes de que el nodo de control tenga que cortar nada).
- `py -m unittest discover -s tests/host` → 9 pruebas OK.

---

---

## 7. Crecimiento sin cota del almacén de logs (límite conocido; decidido en D15.13)

**Estado:** aceptado como límite conocido el 2026-11-18, **decidido** el
2026-11-21 en D15.13 (§7.6) e **implementado** ese mismo día en el Commit 12 de
§6. No fue un descuido de implementación: era una política que no existía, y
elegirla por omisión es justo lo que prohíbe la regla 7 del proyecto. Lo que
sigue describe la situación tal como estaba al abrir el análisis.

### 7.1 Lo que hoy no existe

- Nada borra los archivos de `/var/lib/dpes/logs`: crecen con cada trabajo, cada
  intento y cada reintento, para siempre.
- No hay tope de bytes por intento. El único límite es el máximo de un fragmento
  (4 GiB, el campo de longitud del formato, D15.2).
- El índice en memoria tampoco se libera: un `LogDir` recuerda cada intento
  abierto hasta que el actor muere o se reinicia (D15.12).
- La pasarela no acota lo que sirve: el `limit` lo elige el cliente (D15.4).

### 7.2 Por qué es una vulnerabilidad del sistema

1. **El disco lo consume el trabajo, no el sistema.** El comando lo escribe el
   cliente y nada impide un `while True: print(...)`. No hace falta un atacante:
   basta un bucle de depuración olvidado.
2. **Se degrada en cascada.** El volumen `dpes-data` guarda, en el mismo nodo de
   control, los blobs (plan/11), `results.sqlite` (plan/12) y
   `coordinator.sqlite` (plan/14). Un disco lleno no pierde «solo logs»: falla
   el `put` de artefactos, falla el punto de confirmación y el coordinador deja
   de poder anotar su diario. Una tarea vuelve poco fiable al nodo de control
   entero.
3. **La Fase 4 agrava el problema a propósito.** Antes, la salida vivía en un
   temporal del trabajador y del intento solo se conservaban 64 KiB (D07.14).
   Desde el Commit 3, cada fragmento se copia al volumen y se queda: cumplir F5.4
   (lo transmitido sobrevive al trabajador) y no acotar el volumen son la misma
   decisión vista desde dos lados.
4. **Es rápido.** Medido en la máquina de desarrollo: 604 MB/s escribiendo
   bloques de 4 KiB sin búfer a un archivo (623 MB en un segundo). A ese ritmo,
   10 GB son menos de veinte segundos de escritura sostenida; la caché del
   sistema difiere la descarga al disco, no la evita.
5. **El fallo llega por dos caminos.** (a) El volumen del nodo de control, con
   los fragmentos ya enviados; (b) el disco del trabajador, porque el ejecutor
   guarda la salida completa en un temporal del que el emisor de D15.3 va
   leyendo, y el archivo no se recorta al leerlo: mide lo que mida toda la
   salida del comando mientras el intento dura. D15.3 no define tope de salida,
   así que hoy no hay contrapresión en ninguno de los dos lados.

### 7.3 Opciones

| # | Política | Qué protege | Qué cuesta | Qué rompe |
|---|----------|-------------|-----------|-----------|
| 1 | **Tope por intento** (N MiB, en `LogDir.append`) | Volumen y trabajador | Un contador por intento y una marca de corte | La cola del log: hay que decidir si se avisa (evento + indicador) o el cliente creería que la salida terminó ahí |
| 2 | **Retención por trabajo**: al cerrar, borrar los archivos de los intentos perdedores | Volumen a largo plazo | Un barrido y decidir quién lo hace | Los logs de los intentos fallidos, que son los más útiles para depurar, si no se conservan unos cuantos |
| 3 | **Rotación por tamaño o antigüedad** en el actor | Todo | Índice por segmento, offsets estables, borrar mientras alguien lee | Los offsets de C4 (`logs -o N` exacto) si la numeración de la salida cambia al rotar |
| 4 | **Límite fuera del sistema**: cuota del volumen (`--storage-opt size=`, `tmpfs size=`) o barrido externo | El anfitrión | Nada de código; configuración y documentación | No evita el fallo, lo convierte en `ENOSPC` (que el almacén ya trata como `OSError`, D15.2) y deja al sistema sin logs |
| 5 | **Tope de la captura en el trabajador**: el emisor deja de enviar y el archivo se trunca al pasar de N MiB | El disco del trabajador | Un contador en el emisor y una decisión sobre qué hace con el archivo | Lo que se sirve deja de coincidir con lo que el comando escribió, y truncar mueve los offsets de D15.4 |

No son excluyentes: 1 protege el volumen del nodo de control, 2 lo limpia con el
tiempo y 5 el disco del trabajador; entre las tres cubren los dos discos y las dos
escalas de tiempo sin tocar el formato de D15.2.

### 7.4 Preguntas que la decisión debe responder

- ¿Tope por intento, retención por trabajo o las dos? ¿Por intento, por trabajo
  o por nodo?
- Cuando se corta: ¿se descartan los fragmentos siguientes y se avisa, o se
  rechazan desde el principio?
- ¿Se conservan los logs de los intentos perdedores? ¿Cuántos y durante cuánto?
- ¿Quién barre: el actor (perezoso, al abrir o escribir), el coordinador (al
  cerrar el trabajo) o algo externo al sistema?
- ¿Qué ve un `tail` en curso cuando el archivo se trunca o se borra? Hoy `read`
  devolvería `[]` y la cola acabaría por inactividad (D15.4), o al instante si
  el intento ya estaba cerrado (D15.14); si eso debe distinguirse de «no hay más
  salida», hace falta una señal.
- ¿Cuenta el tope bytes de salida o también el índice (~145 B por fragmento)?

### 7.5 Objetivo

Tomar la decisión (D15.13) y ejecutarla en un paso propio, con su corte y sus
pruebas. La implementación mínima previsible:

- contador de bytes por intento en `LogDir.append`, con un motivo de corte
  explícito en lugar de un `ValueError` genérico;
- evento de sistema «log truncated» con `execution=` (D10.5), para que la
  truncación sea visible y no silenciosa;
- prueba unitaria del tope y de que el almacén sigue aceptando una `seq` que
  salta un hueco (D15.1);
- prueba de que un trabajo normal no cambia de comportamiento ni de rendimiento.

**No forma parte del corte C4.** C4 se cierra con lo que hay: D15.2 y D15.12
defienden la durabilidad, no la retención. El apartado y el riesgo
correspondiente de [00-contexto-y-restricciones.md](00-contexto-y-restricciones.md)
§6 fueron el registro de la deuda hasta D15.13.

### 7.6 Decisión D15.13 — dos topes por intento y barrido al cerrar el trabajo

**Contexto.** El de §7.1 y §7.2: dos discos expuestos (el volumen del nodo de
control, que comparte con blobs, resultados y diario; y el temporal del
trabajador), ninguna política de retención, y un emisor que hoy no tiene
contrapresión en ninguno de los dos lados. El corte C4 cerró con esa deuda
declarada; este paso la salda.

**Opciones consideradas.** Las cinco de §7.3, con su coste y su rotura. La 3
(rotación) se descarta porque mueve los offsets que C4 garantiza (`logs -o N`
lee bytes exactos del intento) y obligaría a un índice por segmento. La 4
(cuota del volumen) no evita el fallo: lo convierte en `ENOSPC` y deja al
sistema sin logs, que es lo que D15.2 ya trata como `OSError`. La 1, la 2 y la 5
no son excluyentes, y entre las tres cubren los dos discos y las dos escalas de
tiempo sin tocar el formato de D15.2.

**Decisión.**

1. **Tope por intento en el nodo de control** (`LogDir.append`): un intento
   guarda como mucho `MAX_ATTEMPT_BYTES` (100 MiB por omisión,
   `DPES_LOG_MAX_MIB`). Un fragmento que no cabe no se escribe y `append` lanza
   `LogTruncated`, con `execution_id`, `size` y `limit`; el actor lo registra
   como evento de sistema `log truncated` con `execution=` (D10.5) antes de
   relanzarlo. La comprobación de duplicado (D15.1) va antes que la del tope:
   reintentar un `seq` que ya está no puede confundirse con una truncación.
2. **Tope de la captura en el trabajador** (`MAX_CAPTURE_BYTES`, 50 MiB por
   omisión, `DPES_LOG_CAPTURE_MIB`), en dos mitades que se complementan: el
   emisor deja de enviar al llegar al tope, y `sandbox.run` **mata el comando**
   cuando el archivo de captura lo pasa. Truncar el archivo no sirve: el hijo
   escribe con el mismo desplazamiento que el padre comparte, así que tras
   `truncate` la siguiente escritura vuelve a extender el archivo hasta ese
   desplazamiento y el disco no se libera. La única forma de acotar el disco del
   trabajador mientras el comando corre es terminarlo.
3. **Retención por trabajo** (`LogStore.drop`, llamado por el coordinador al
   cerrar): de cada tarea se conserva el intento al que apunta la vista (el que
   la resolvió, o el último fallido, que es el que interesa depurar) y se
   borran los demás —los reintentos superados y los intentos de rondas viejas—.
   Un intento descartado queda marcado: un fragmento tardío no lo recrea
   (`append` devuelve `False`), y un seguidor suyo termina en el acto en vez de
   esperar `IDLE_S`, en lugar de encontrarse un archivo que ya no está.
4. **Visibilidad.** `TaskResult.log_truncated` (campo nuevo con valor por
   omisión, aditivo: los constructores actuales no cambian) viaja del ejecutor
   al estado, y la vista de la tarea expone `truncated`. El motivo del fallo lo
   dice: «output over the log limit». El evento de sistema lleva
   `execution=`, `side` (`control` o `worker`), `bytes` y `limit`.

**Porqué.**

- El tope del nodo de control es el que protege lo que de verdad duele: el
  volumen que comparte con la caché de artefactos, el punto de confirmación y el
  diario del coordinador. Falla rápido y en el sitio donde el daño es en cadena.
- El tope del trabajador evita el segundo camino. Sin matar el comando, un
  `while True: print()` llena el disco del trabajador en segundos y ningún tope
  del nodo de control lo impide, porque el archivo crece antes de que el emisor
  lo lea.
- El barrido no puede ser inmediato como los topes: el trabajo puede seguir
  sirviendo sus logs mientras el cliente los lee, así que solo barre al cerrar,
  y solo lo que la vista ya no puede pedir.
- Los topes son por intento y no por trabajo: un trabajo grande con muchas
  tareas no se ve penalizado por una tarea habladora, y el reintento de un
  intento truncado vuelve a tener su propio presupuesto acotado.

**Consecuencias.**

- Un comando que pasa del tope del trabajador muere por señal (`exit_code`
  negativo) y la tarea falla con «output over the log limit»; el reintento, si
  lo hay, repite el trabajo acotado. Es una decisión de política, no un fallo
  del sistema: por eso la tarea falla y no el trabajador.
- Los offsets de C4 no cambian: lo truncado nunca se guardó, así que el tamaño
  del intento sigue siendo el de los bytes aceptados, y lo que se sirve es lo
  que hay. Un `tail` en curso acaba porque el intento se cierra igual (D15.14).
- Los valores por omisión (100 y 50 MiB) dejan intacto cualquier trabajo normal:
  un intento que no llega al tope produce los mismos fragmentos, los mismos
  offsets y los mismos eventos que antes; solo aparece `truncated: false`.
- El barrido no se anota en el diario: es espacio, no estado. Es idempotente y
  de mejor esfuerzo; si el actor no contesta, se registra `log sweep failed` y
  el trabajo no cambia. Un trabajo recuperado tras un reinicio del coordinador
  (`_log_if_finished` sale antes para esos, D14.4) no se barre: sus logs se
  quedan, y ese resto lo absorbe la política de retención del trabajo siguiente
  que comparta intento, o el tope por intento. Queda declarado aquí.
- El `LogDir` guarda el conjunto de ids descartados (una cadena por intento
  barrido) para no recrear archivos borrados; es memoria pequeña y crece con
  los reintentos, no con los bytes.
- Cerrado el riesgo de [00-contexto-y-restricciones.md](00-contexto-y-restricciones.md)
  §6 en lo que respecta a crecimiento sin tope; lo que queda es la elección de
  los números, que es configuración.

**Pruebas.** Unitarias del tope, del hueco de `seq` y del barrido
(`tests/unit/test_logstore.py`), del emisor y del comando terminado por el tope
(`tests/unit/test_executor.py`), del `sandbox` matando por captura
(`tests/unit/test_sandbox.py`), de la vista con `truncated`
(`tests/unit/test_scheduler.py`); de integración, la truncación atravesando el
actor de Ray (`tests/integration/test_logstore.py`); de extremo a extremo, un
trabajo que imprime sin pausa y acaba acotado y con `truncated: true`
(`tests/e2e/test_logs.py`).