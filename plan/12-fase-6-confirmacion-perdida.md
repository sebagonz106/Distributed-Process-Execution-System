# 12 — Fase 6: la tarea terminó, pero se perdió la confirmación

- **Fecha:** 2026-11-15
- **Estado:** en curso
- **Quién:** persona A (Juan Carlos), pista de control
- **Paso:** Fase 6 de [03-fases-y-dependencias.md](03-fases-y-dependencias.md)
- **Requisito:** R7 (tratamiento del caso «la tarea terminó, pero se perdió la
  confirmación»)
- **Corte:** C6 — con el fallo «terminar justo después de guardar el
  resultado», el trabajo termina `SUCCEEDED`, el comando se ejecutó una sola
  vez y el historial registra el resultado adoptado por reconciliación; una
  confirmación duplicada o tardía no cambia nada; reenviar un trabajo con la
  misma clave de idempotencia devuelve el mismo identificador.

## 1. Alcance

| Feature | Qué | Estado |
|---------|-----|--------|
| F7.1 | Identidad: identificador de ejecución y clave de resultado | Hecho en la Fase 1 (D06.2, D07.5) |
| F7.2 | Punto de confirmación: el resultado se guarda antes de avisar | Hecho |
| F7.3 | Guardado idempotente: el primero que escribe gana | Hecho |
| F7.4 | Reconciliación antes de reintentar | Hecho |
| F7.5 | Confirmaciones tardías o duplicadas, sin efecto | Hecho (D07.6, ampliado en D12.5) |
| F7.6 | El ejecutor reintenta el guardado con el mismo identificador | Hecho |
| F1.6 | Envío idempotente (S) | Hecho |
| F7.7 | Recuperación del coordinador tras reiniciarse (S) | Fase 8 |

Esta fase toca dos piezas de la pista B: el ejecutor (guarda antes de
contestar) y el actor del almacén (guarda los resultados). Es lo que D02.10
pone en esta fase; se avisa a la persona B.

## 2. Decisiones

### D12.1 — Los resultados confirmados viven en SQLite, en el almacén

- **Decisión:** el actor del almacén (el de los blobs, D11.5) gana una tabla
  SQLite `results` en el mismo volumen del nodo de control, con clave primaria
  `(trabajo, tarea, ronda)` (`ResultKey`, D02.10). Guardar es
  `INSERT OR IGNORE` seguido de leer la fila: **el primero que escribe gana** y
  quien guarda recibe siempre el resultado ganador. El resultado se guarda como
  JSON. El código vive en `dpes/store.py`.
- **Opciones:** un actor nuevo solo para resultados; el mismo actor del
  almacén; el coordinador como único que guarda.
- **Porqué:** D02.4 ya eligió SQLite y D02.10 pone los resultados en el
  almacén, no en el coordinador: si guardara el coordinador, un resultado cuya
  confirmación se pierde no estaría en ningún sitio. Un solo actor para blobs y
  resultados mantiene un dueño por componente. La unicidad no depende de que el
  actor procese de uno en uno: la da la clave primaria de SQLite, que es atómica
  incluso entre procesos.
- **Durabilidad:** SQLite confirma cada escritura en disco antes de contestar,
  y el volumen sobrevive a reiniciar el contenedor. Un resultado contestado
  como guardado no se pierde aunque el nodo de control se reinicie.
- **JSON en vez de pickle:** se puede leer con cualquier cliente de SQLite al
  depurar, y no ejecuta código al cargarse.

### D12.2 — Punto de confirmación: el ejecutor guarda antes de contestar

- **Decisión:** el ejecutor guarda todo `TaskResult`, sea un éxito o un fallo
  del comando, **antes** de devolverlo, y devuelve el ganador. Un
  `ExecutorFailure` no se guarda: no hay resultado del comando.
- **Porqué:** desde el guardado la tarea terminó para el sistema, llegue o no
  la confirmación al coordinador. Se guarda también el fallo del comando porque
  es un hecho igual de definitivo: sin guardarlo, una confirmación perdida de un
  `exit 1` haría repetir un comando que ya falló.
- **El ganador puede ser de otro intento:** si un intento anterior de la misma
  ronda guardó primero, el ejecutor devuelve *su* resultado. Los dos caminos
  llegan al mismo resultado (regla 5 de D02.10).

### D12.3 — El ejecutor reintenta el guardado con la misma clave

- **Decisión:** si guardar falla, el ejecutor lo reintenta tres veces más,
  tras 0,5, 1 y 2 s, con la misma clave y el mismo resultado (F7.6). Agotados,
  devuelve `ExecutorFailure(RESULT_SAVE_FAILED)`, que es `NETWORK` (D09.1).
- **Porqué:** repetir el guardado es inofensivo por D12.1, así que reintentar
  solo puede ayudar. Un fallo del guardado no dice si se guardó: quizá se
  perdió la respuesta. Por eso el coordinador reconcilia también este caso
  (D12.4).

### D12.4 — Reconciliación antes de dar un intento por fallido

- **Decisión:** cuando un intento termina sin resultado (`WORKER`, `NETWORK`,
  plazo vencido, ruptura del contrato), el coordinador consulta la clave del
  intento en el almacén **antes** de pasar el fallo a la máquina de estados. Si
  hay resultado guardado, lo adopta: el intento termina con ese resultado y no
  se lanza ningún reintento. Si no lo hay, el fallo sigue su curso (D09.2).
- **Si la consulta falla** (el almacén no contesta), el fallo sigue su curso.
  No se pierde corrección: si había resultado, el reintento lo recibirá al
  guardar, porque la ronda es la misma (D12.2).
- **Porqué:** separa «la tarea terminó» (un hecho durable) de «el coordinador
  se enteró» (un mensaje que puede perderse). Perder el mensaje cuesta una
  consulta, no una ejecución. Es el caso que el experimento de la Fase 0 dejó
  abierto: un trabajador incomunicado puede terminar y guardar justo antes de
  que el coordinador lo dé por perdido (D06.14).

### D12.5 — La máquina de estados resuelve un intento con un resultado ajeno

- **Decisión:** `on_result` recibe el resultado y, aparte, el intento que
  resuelve. Si el resultado lo produjo otro intento (adoptado por
  reconciliación o devuelto como ganador), el historial lo dice en el motivo.
- **Porqué:** antes, un resultado se emparejaba con el intento por su propio
  identificador de ejecución. Un ganador producido por un intento ya
  abandonado se habría ignorado, y el intento en curso no se habría resuelto
  nunca.
- **Duplicados y tardíos** siguen sin efecto (D07.6): una vez resuelto el
  intento, cualquier otra confirmación para él se ignora, sea del mismo
  resultado o de otro.

### D12.6 — Fallo inyectable `lose-ack`

- **Decisión:** `DPES_FAULT=lose-ack:<n>` hace que el ejecutor guarde el
  resultado y termine su proceso sin contestar, en los primeros *n* intentos.
  Se suma a los tipos de D09.7.
- **Porqué:** es exactamente el caso de R7, en el instante exacto, y
  reproducible en la CI. Desde fuera habría que acertar la ventana de
  milisegundos entre el guardado y la respuesta.

### D12.7 — Envío idempotente con una clave del cliente

- **Decisión:** `POST /jobs` acepta la cabecera `Idempotency-Key` (1 a 64
  caracteres de `[A-Za-z0-9_-]`). La misma clave con el mismo trabajo devuelve
  el mismo identificador sin crear otro. La misma clave con un trabajo distinto
  es un error `400`. El cliente genera una clave por envío y, si no puede
  contactar con la pasarela, reintenta hasta tres veces con la misma clave.
- **Porqué:** el envío es una llamada remota como cualquier otra. Si su
  respuesta se pierde, el cliente no sabe si el trabajo se creó (Lab1); sin
  clave, reintentar lo duplicaría (Lab4). Con la misma clave para un trabajo
  distinto se avisa, porque devolver el anterior ocultaría el error.
- **Límite:** las claves viven en la memoria del coordinador, como sus
  trabajos (D07.8). Se perderían si el coordinador se reiniciara (F7.7, Fase 8).

### D12.8 — Qué se garantiza

- **S1** (una tarea no tiene más de un resultado confirmado por ronda) la da la
  clave primaria de D12.1.
- El **comando** se ejecuta *al menos una vez*: si el ejecutor muere antes de
  guardar, se repite. El **resultado** se confirma *como mucho una vez* por
  ronda. El cliente ve un único resultado. Es la garantía de 02 §1, ahora con
  mecanismo.

## 3. Pruebas

| Nivel | Fichero | Qué comprueba |
|-------|---------|---------------|
| Unitarias | `tests/unit/test_store.py` | Gana el primero y todos lo saben; guardar dos veces es inofensivo; cada ronda tiene su resultado; el resultado sobrevive a reabrir la base; dos conexiones sobre el mismo fichero acuerdan un único ganador (D12.1) |
| Unitarias (Linux) | `tests/unit/test_executor.py` | Se guarda antes de devolver, también un fallo del comando; se devuelve el ganador de otro intento; el guardado se reintenta con la misma clave y el mismo resultado; agotado, es `RESULT_SAVE_FAILED`; un fallo del ejecutor no guarda nada; `lose-ack` termina después de guardar (D12.2, D12.3, D12.6) |
| Unitarias | `tests/unit/test_scheduler.py` | Un resultado adoptado resuelve el intento en curso y lo dice en el historial; un fallo de la tarea adoptado sigue siendo `TASK`; los duplicados tras la adopción no cambian nada (D12.5) |
| Unitarias | `tests/unit/test_gateway.py` | La cabecera llega al coordinador; una clave mal formada se rechaza; el cliente reintenta con la misma clave y se rinde tras tres intentos (D12.7) |
| Integración | `tests/integration/test_coordinator.py` | Un resultado guardado cuya respuesta se pierde se adopta sin repetir; la misma clave da el mismo trabajo; reutilizarla para otro se rechaza |
| Extremo a extremo | `tests/e2e/test_lost_ack.py` | **Corte C6** por la pasarela, con `lose-ack` en el ejecutor real |

Estado el 2026-11-15, todo en verde: 197 unitarias en el contenedor (159 en
Windows), 17 de integración, 22 de extremo a extremo y 7 desde el anfitrión.

En el corte C6, el trabajo con `lose-ack` termina `SUCCEEDED` con un único
intento cuyo motivo es «adopted by reconciliation after WorkerCrashedError»: el
comando se ejecutó una sola vez.
