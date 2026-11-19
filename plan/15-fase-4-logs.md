# 15 — Fase 4: logs de tareas en vivo y historial de eventos

- **Fecha:** 2026-11-18
- **Estado:** en curso (Commit 2: actor `LogStore` y su núcleo en disco)
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
- El comando y el emisor no pueden compartir un descriptor con una sola posición: el ejecutor crea el archivo de salida, lo deja sin nombre (`unlink` inmediato) y lo lee por un segundo descriptor con posición propia. Sandbox pasa a aceptar el archivo del llamante (`sink`) en vez de crear siempre el suyo.
- Un almacén que no acepta un fragmento ni tras los tres reintentos se da por caído: el emisor deja de enviar el resto de ese intento (un log no puede tumbar un trabajo) y lo declara con un `WARNING` con `execution=` y `lost=`. Reintentar cada fragmento de un almacén muerto solo retrasaría la tarea.
- El ejecutor espera al emisor como mucho `JOIN_S = 3` s, por debajo del margen `RUN_MARGIN_S = 5` s del coordinador (D09.4); si el hilo sigue vivo, es *daemon* y termina solo.

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
   `\x00DPES-EOF`): se mezcla con la salida, y una tarea puede escribir esos
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

---

## 3. Contratos entre componentes (resumen)

| Productor | Consumidor | Contrato | Notas |
|-----------|------------|----------|-------|
| Executor (hilo emisor) | LogStore (actor) | \`append_fragment(eid, seq, bytes) -> bool\` | Idempotente por seq |
| Executor (hilo emisor) | LogStore (actor) | \`close_fragments(eid) -> bool\` | Marca el fin (D15.14) |
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

Y añadir decisiones D15.1–D15.12 y D15.14 al registro de decisiones
abiertas/cerradas (D15.13 queda para §7).

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

---

## 7. Crecimiento sin cota del almacén de logs (límite conocido, decisión pendiente)

**Estado:** aceptado como límite conocido el 2026-11-18 y **sin decidir**. No es
un descuido de implementación: es una política que no existe, y elegirla por
omisión es justo lo que prohíbe la regla 7 del proyecto. El número **D15.13**
queda reservado para la decisión.

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
defienden la durabilidad, no la retención. Mientras no se decida, este apartado
y el riesgo correspondiente de [00-contexto-y-restricciones.md](00-contexto-y-restricciones.md)
§6 son el registro de la deuda; no se implementa nada por omisión.