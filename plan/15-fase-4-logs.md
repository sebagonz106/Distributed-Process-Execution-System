# 15 — Fase 4: logs de tareas en vivo y historial de eventos

- **Fecha:** 2026-11-18
- **Estado:** en curso (Commit 1: decisiones y contratos)
- **Paso:** Fase 4 de [03-fases-y-dependencias.md](03-fases-y-dependencias.md)
- **Requisito:** R5 (registro y transmisión de logs)
- **Corte:** C4 — \`logs -f\` muestra líneas mientras la tarea corre; tras matar el worker a mitad, lo emitido sigue consultable; \`events\` muestra historial con execution IDs, transiciones, clases de fallo y esperas de reintento

## 1. Alcance

| Feature | Qué | Estado |
|---------|-----|--------|
| F5.1 | Captura de stdout/stderr de la tarea | ✅ Ya existe (output_tail 64 KiB, D07.14) |
| F5.2 | Envío incremental durante la ejecución, en fragmentos numerados | 🟡 Este documento |
| F5.3 | Consulta por desplazamiento y seguimiento en vivo (\`logs -f\`) | 🟡 Este documento |
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
- La idempotencia en el almacén (D15.2) requiere una clave simple: \`(execution_id, seq)\` como clave primaria.
- Permite al cliente pedir "desde seq N" y al almacén ignorar duplicados exactos.
- El orden natural de enteros coincide con el orden causal de emisión.
- Mínima sobrecarga: 4-8 bytes por fragmento en metadatos.

**Consecuencias:**
- El ejecutor debe mantener contador local por intento y persistirlo si se reiniciara (no aplica: el ejecutor no se reinicia a mitad de intento).
- El almacén debe rechazar/ignorar escrituras con \`seq <= max_seq_existente\` para ese execution_id.

---

### D15.2 — Persistencia del almacén de logs (LogStore)

**Contexto:** Los fragmentos deben sobrevivir a la muerte del worker (F5.4) y estar disponibles para consulta por offset y streaming en vivo (F5.3). El volumen \`dpes-data\` en el nodo de control ya existe y persiste entre reinicios de contenedor.

**Opciones consideradas:**
1. **Archivo por intento:** \`/var/lib/dpes/logs/{execution_id}.log\` — escritura append-only, índice en memoria (mapa seq → byte_offset). Lectura por offset = seek directo.
2. **SQLite:** Tabla \`fragments(execution_id, seq, data, offset)\`. Transacciones ACID, consultas flexibles, pero más overhead por fragmento.
3. **Archivo por fragmento:** \`/var/lib/dpes/logs/{execution_id}/seq_{N}.chunk\`. Simple pero muchos inodes y syscalls.

**Decisión:** Opción 1 — un archivo por intento (\`{execution_id}.log\`) con índice en memoria reconstruido al abrir.

**Porqué:**
- Append-only es el patrón de acceso natural (escritura secuencial, lectura por offset).
- Seek en archivo es O(1) y extremadamente rápido; no hace falta B-tree para este caso de uso.
- Un archivo por intento = limpieza trivial (borrar archivo = borrar logs de un intento).
- Evita overhead de SQLite (parseo SQL, transacciones) para un workload de "write once, read many" simple.
- El volumen \`dpes-data\` ya está montado en \`/var/lib/dpes\`; solo crear subdirectorio \`logs/\`.

**Consecuencias:**
- El índice en memoria (mapa \`seq -> file_offset\`) se reconstruye escaneando el archivo al abrir (rápido: lectura secuencial una sola vez).
- Tamaño máximo de archivo = salida total de la tarea. Si una tarea produce GBs, el archivo crece sin límite (documentar como límite conocido, F5.4 cubre supervivencia, no rotación).
- Rotación/retención de logs queda fuera de alcance (Fase 8 o post-entrega).

**Estructura del archivo:**
```
[header: magic=0x44504553 ("DPES"), version=1, execution_id_len, execution_id]
[fragment_1: seq=1 (u64), data_len=L1 (u32), data[L1]]
[fragment_2: seq=2 (u64), data_len=L2 (u32), data[L2]]
...
```
- Binario, little-endian. Permite parsing rápido y seek exacto.
- \`data\` = bytes raw de stdout/stderr (UTF-8 válido asumido, pero almacenamos bytes).

---

### D15.3 — Política de emisión desde el ejecutor

**Contexto:** El ejecutor captura stdout/stderr en un archivo temporal (ya implementado en D07.15). Debe leer ese archivo incrementalmente y enviar fragmentos al LogStore sin bloquear la ejecución del comando.

**Opciones consideradas:**
1. **Hilo dedicado (threading.Thread):** Lee el archivo en bucle, envía fragmentos cada N bytes / M líneas / T ms. Simple, estándar en Python.
2. **Asyncio + aiofiles:** Requiere que el ejecutor sea async; hoy es síncrono (D07.17). Cambio mayor.
3. **Polling desde el coordinador:** El coordinador pide fragmentos al ejecutor. Invierte dependencias, añade latencia y complejidad.

**Decisión:** Opción 1 — hilo dedicado (\`threading.Thread\`) en el ejecutor.

**Porqué:**
- El ejecutor ya es síncrono y no conoce asyncio; un hilo es el cambio mínimo.
- \`threading\` está en la biblioteca estándar (sin dependencias extra).
- El hilo puede ser daemon: si el proceso principal termina, el hilo muere sin bloquear.
- Permite batching configurable: \`flush_bytes=4096\`, \`flush_lines=100\`, \`flush_interval_ms=500\` (lo que ocurra primero).

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
- El ejecutor debe importar \`threading\` (ya lo hace para \`signal\` en sandbox).
- El cliente de almacenamiento (\`_StoreClient\`) gana método \`append_fragment(execution_id, seq, data)\`.
- Si el hilo falla al enviar (red, store caído), loggea warning y reintenta con backoff simple (3 intentos, 0.5s). No bloquea la ejecución del comando.

---

### D15.4 — API de logs en la pasarela (Gateway)

**Contexto:** El cliente necesita consultar logs por offset (histórico) y seguirlos en vivo (\`logs -f\`). La pasarela es el único punto de entrada HTTP (puerto 8000).

**Opciones consideradas:**
1. **Dos endpoints separados:** \`GET /logs?offset=N&limit=M\` y \`GET /logs?follow=1\`.
2. **Un endpoint con query params:** \`GET /logs?offset=N&limit=M&follow=0/1\`.
3. **Range header HTTP estándar:** \`GET /logs\` con \`Range: bytes=START-\` y \`Accept: text/plain\`; streaming con \`Transfer-Encoding: chunked\`.

**Decisión:** Opción 2 (un endpoint con params) + soporte opcional de \`Range\` header (Opción 3 como bonus).

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
- \`text/plain; charset=utf-8\` — bytes directos de stdout/stderr.
- El cliente reconstruye líneas; no añadimos delimitadores extra.

**Soporte \`Range\` (bonus, no bloqueante):**
```http
GET /jobs/{job_id}/tasks/{task}/logs
Range: bytes=1000-
→ 206 Partial Content, Content-Range: bytes 1000-1999/2000
```
Útil para clientes HTTP genéricos (curl, wget).

**Manejo de desconexión en streaming:**
- El handler detecta \`GeneratorExit\` / \`BrokenPipeError\` / \`ConnectionResetError\`.
- Al detectar, cancela la suscripción al \`tail_fragments\` del LogStore y libera recursos.
- No deja hilos/corutinas huérfanas.

**Consecuencias:**
- La pasarela necesita acceso al actor \`LogStore\` (ya tiene acceso al \`BlobStore\` y \`Coordinator\`).
- \`RAY_DEDUP_LOGS=0\` en compose (ya configurado, D10.7) evita que Ray fusione líneas de distintos intentos.

---

### D15.5 — API de historial de eventos en la pasarela

**Contexto:** F5.5 requiere historial de eventos del trabajo: transiciones de estado, intentos, clase de fallo, decisión de reintento, correlacionados por execution ID. Ya existe el historial de intentos en la vista del trabajo (D09.6), pero F5.5 pide un feed cronológico unificado servido por HTTP.

**Decisión:** Endpoint \`GET /jobs/{job_id}/events\` que devuelve array JSON ordenado cronológicamente.

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

**Tipos de evento y cuándo se emiten (desde \`scheduler.Job\`, D15.6):**
| Tipo | Cuándo | Payload clave |
|------|--------|---------------|
| \`TASK_LAUNCHED\` | \`start()\` o \`_launch()\` | \`state: "RUNNING"\` |
| \`ATTEMPT_SUCCEEDED\` | \`on_result()\` con éxito | \`state: "SUCCEEDED", exit_code\` |
| \`ATTEMPT_FAILED\` | \`on_result()\` fallo tarea / \`on_failure()\` | \`failure_class, reason, retry_in_s\` |
| \`RETRY_SCHEDULED\` | \`_failed()\` programa reintento | \`failure_class, retry_in_s, next_execution_id\` |
| \`JOB_FINISHED\` | \`_log_if_finished()\` | \`state: "SUCCEEDED|FAILED|ERROR", counts\` |

**Persistencia:** Los eventos viven en memoria en \`scheduler.Job._events\` y se reconstruyen al reiniciar el coordinador vía journal (D14.1). El endpoint \`events()\` del actor coordinador lee \`job._events\`.

**Consecuencias:**
- Los eventos son **datos**, no logs operativos (D10.4). Los módulos puros no logean; el coordinador emite eventos como estructuras de datos.
- Timestamp en ISO 8601 UTC con milisegundos (generado en el coordinador al emitir).
- El endpoint es de solo lectura; no requiere idempotency key.

---

### D15.6 — Emisión de eventos desde el scheduler (núcleo puro)

**Contexto:** La máquina de estados pura (\`scheduler.Job\`) debe emitir eventos de datos (no logs) en cada transición relevante. Estos eventos alimentan el endpoint \`/events\` (D15.5) y complementan el historial de intentos (D09.6).

**Decisión:** Extender \`scheduler.Job\` con lista \`_events\` y método \`_record_event(type, execution_id, **payload)\`.

**Implementación:**
```python
@dataclass
class Event:
    timestamp: float      # time.monotonic() — se convierte a ISO en capa HTTP
    type: str
    execution_id: str
    payload: dict

class Job:
    def __init__(self, ...):
        self._events = []

    def _record_event(self, type: str, execution_id: str, **payload):
        self._events.append(Event(time.monotonic(), type, execution_id, payload))

    # Puntos de llamada:
    # start() / _launch()           -> TASK_LAUNCHED
    # on_result() success           -> ATTEMPT_SUCCEEDED
    # on_result() failure (TASK)    -> ATTEMPT_FAILED (failure_class=TASK)
    # on_failure() (WORKER/NETWORK) -> ATTEMPT_FAILED (failure_class=...)
    # _failed() con reintento       -> RETRY_SCHEDULED
    # _log_if_finished()            -> JOB_FINISHED
```

**Consecuencias:**
- \`scheduler.py\` sigue siendo **puro**: no importa \`time\` en producción (inyectado en tests), no escribe disco, no usa Ray.
- Los tests unitarios verifican: cada transición emite evento correcto, payload completo, orden cronológico.
- El coordinador (actor Ray) expone \`events(job_id)\` que devuelve \`job._events\` convertido a JSON serializable.

---

### D15.7 — CLI: comando \`logs\`

**Contexto:** El usuario necesita consultar logs históricos y seguirlos en vivo desde la línea de comandos.

**Decisión:** Nuevo subcomando \`logs\` con flags \`-f/--follow\`, \`-o/--offset\`, \`-n/--lines\`.

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

**Implementación en \`dpes/cli.py\`:**
- \`follow=False\`: \`GET /jobs/{id}/tasks/{task}/logs?offset={o}&limit={n}\` → imprime cuerpo y sale.
- \`follow=True\`: \`GET /jobs/{id}/tasks/{task}/logs?follow=1&offset={o}\` con \`stream=True\` → itera chunks e imprime en tiempo real. Maneja \`KeyboardInterrupt\` limpio.

**Códigos de salida:**
- 0: éxito
- 1: error HTTP (4xx, 5xx)
- 2: job/task no encontrado (404)
- 3: error de conexión / gateway inalcanzable

---

### D15.8 — Contrato del cliente de almacenamiento (_StoreClient)

**Contexto:** El ejecutor y la pasarela necesitan hablar con el actor \`LogStore\`. El patrón existente (\`_StoreClient\` en \`coordinator.py\`) envuelve llamadas Ray con timeouts.

**Decisión:** Añadir método \`append_fragment(execution_id, seq, data)\` a \`_StoreClient\`.

```python
class _StoreClient:
    # ... existing methods ...
    def append_fragment(self, execution_id: str, seq: int, data: bytes) -> bool:
        reply = self._actor.append_fragment.remote(execution_id, seq, data)
        return ray.get(reply, timeout=CALL_TIMEOUT_S)
```

**Timeout:** \`CALL_TIMEOUT_S = 10\` (igual que otras llamadas). Si falla, el hilo emisor del ejecutor reintenta 3× con backoff 0.5s (D15.3).

---

### D15.9 — Manejo de deduplicación de logs de Ray

**Contexto:** Ray por defecto deduplica líneas de log que difieren solo en números (D10.7). Los execution IDs contienen números (\`job/task/round/attempt\`), así que Ray fusionaría líneas de intentos distintos.

**Decisión:** Confirmar \`RAY_DEDUP_LOGS=0\` en \`docker/compose.yaml\` para el servicio \`head\` (ya está, D10.7). Documentar en D15.1 que el almacén usa su propia secuencia, no la de Ray.

**Verificación:** Test unitario en C1 que comprueba que dos intentos distintos con logs idénticos producen líneas separadas en \`docker logs dpes-head\`.

---

### D15.10 — Módulos puros no registran logs (refuerzo D10.4)

**Contexto:** D10.4 establece que \`spec\`, \`dag\`, \`failures\`, \`scheduler\`, \`messages\` no escriben logs operativos. La Fase 4 añade \`logstore\` (no puro) y extiende \`scheduler\` con eventos (datos, no logs).

**Decisión:** Reafirmar D10.4. \`scheduler.Job._record_event\` emite **eventos de datos** (estructura \`Event\`), no llamadas a \`logging\`. La capa HTTP (\`gateway.py\`) y el coordinador (\`coordinator.py\`) son quienes logean eventos operativos si procede (p.ej., \`job finished\` ya se logea en D10.11).

**Consecuencia:** Los tests unitarios de \`scheduler\` no necesitan capturar logs; verifican la lista \`_events\` directamente.

---

## 3. Contratos entre componentes (resumen)

| Productor | Consumidor | Contrato | Notas |
|-----------|------------|----------|-------|
| Executor (hilo emisor) | LogStore (actor) | \`append_fragment(eid, seq, bytes) -> bool\` | Idempotente por seq |
| LogStore (actor) | Gateway | \`read_fragments(eid, offset, limit) -> [(seq, bytes)]\` | Offset en bytes |
| LogStore (actor) | Gateway | \`tail_fragments(eid, offset) -> AsyncIterator[(seq, bytes)]\` | Para streaming |
| Scheduler (núcleo) | Coordinador (actor) | \`job._events -> List[Event]\` | Datos puros, sin Ray |
| Coordinador (actor) | Gateway | \`events(job_id) -> List[Event]\` | Serializado a JSON |
| Gateway | Cliente CLI | HTTP \`GET /logs?offset=&limit=\` / \`?follow=1\` | text/plain, chunked |

---

## 4. Pruebas de aceptación del corte C4 (para documentar en este plan)

| Test | Qué verifica | Nivel |
|------|--------------|-------|
| \`test_live_tail_shows_output_while_running\` | \`logs -f\` imprime salida antes de que la tarea termine | E2E |
| \`test_logs_survive_worker_death\` | \`logs -o 0\` devuelve fragmentos tras \`docker kill worker\` | E2E |
| \`test_events_shows_history\` | \`GET /events\` devuelve transiciones, clases, esperas | E2E |
| \`test_offset_resumption\` | \`logs -o N\` retoma exactamente en byte N | E2E |
| \`test_duplicate_fragment_ignored\` | LogStore ignora seq duplicado | Unit |
| \`test_scheduler_emits_all_events\` | Cada transición genera Event correcto | Unit |
| \`test_gateway_streaming_cleanup\` | Desconexión cliente limpia suscripción tail | Integration |

---

## 5. Actualización de trazabilidad (plan/00-contexto-y-restricciones.md)

Al cerrar esta fase, actualizar tabla §2:

| ID | Funcionalidad | Fases | Estado |
|----|---------------|-------|--------|
| R5 | Registro y transmisión de logs | 1, 4 | ✅ cumplido (C4, 2026-11-22) |

Y añadir decisiones D15.1–D15.10 al registro de decisiones abiertas/cerradas.

---