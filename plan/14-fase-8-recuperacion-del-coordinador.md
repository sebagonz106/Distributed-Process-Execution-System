# 14 — Fase 8, parte A: el coordinador sobrevive a su reinicio

- **Fecha:** 2026-11-16
- **Estado:** en curso. F7.7 está hecha y en verde en el clúster local
  (2026-11-16); falta pasarla por la CI
- **Quién:** persona A (Juan Carlos), pista de control
- **Paso:** Fase 8 de [03-fases-y-dependencias.md](03-fases-y-dependencias.md),
  en la parte que el calendario da a la persona A
  ([04 §2](04-plan-de-implementacion.md)): recuperación del coordinador,
  guiones de fallos en la CI y comparación arquitectónica
- **Requisito:** R7, en su feature F7.7, y la garantía L1 de
  [02 §1](02-arquitectura.md)
- **Corte:** la parte de C8 que depende de F7.7: «reiniciar el coordinador a
  mitad de un trabajo no impide que el trabajo termine»

## 1. Alcance

| Qué | Estado |
|-----|--------|
| F7.7: el coordinador recupera su estado tras reiniciarse y se ocupa de los intentos que dejó en vuelo (S) | Este documento |
| T2: todos los guiones de fallos en la CI | Ya corren los de C5 y C6 y la caída real de un trabajador; se suma el reinicio del coordinador |
| T5: comparación con GitHub Actions, Buildkite y HTCondor | Después, en su propio documento: es un entregable de la orientación |
| Repaso final de `plan/` y versión etiquetada | Al final, cuando estén las Fases 4 y 7 |
| T3: publicación de la imagen | Es de la persona B en el calendario; no se toca sin acordarlo, aunque el informe [13 §9](13-informe-tecnico.md) sugería adelantarla |
| T6: demostración desde un clon limpio | Persona B |

## 2. Decisiones

### D14.1 — El estado del coordinador es el diario de lo que le pasó

- **Contexto:** hasta ahora el estado vivía en memoria (D07.8): un reinicio
  perdía los trabajos y las claves de idempotencia (límite de D12.7). D02.4
  eligió SQLite, con un fichero por dueño. El núcleo es puro y determinista
  (D02.3): `scheduler.Job` solo cambia con `start`, `running`, `on_result` y
  `on_failure`.
- **Opciones:**
  - (a) guardar una foto del estado (vistas y contadores) tras cada cambio;
  - (b) serializar el objeto `Job` con pickle;
  - (c) guardar en orden las llamadas a la máquina de estados y reconstruir el
    estado repitiéndolas;
  - (d) darle el estado al almacén.
- **Decisión:** (c). Un diario en `/var/lib/dpes/coordinator.sqlite`, en el
  volumen del nodo de control, con una fila por llamada:

  | Tipo | Datos | Llamada que repite |
  |------|-------|--------------------|
  | `accepted` | especificación y clave de idempotencia | `Job(...)` y `start()` |
  | `running` | identificador de ejecución | `running()` |
  | `result` | `TaskResult`, identificador y nota | `on_result()` |
  | `failure` | identificador, clase y motivo | `on_failure()` |

- **Porqué:** el estado es función de esas llamadas. Guardarlas es lo mínimo,
  y la reconstrucción usa el mismo código que construyó el estado la primera
  vez. Con (a) habría un segundo modelo del estado que mantener a la par del
  primero. (b) ata el formato del disco a la forma interna de la clase y ejecuta
  código al cargarse: es el motivo por el que D12.1 eligió JSON. (d) rompe «un
  dueño por dato» (02 §3): el almacén no decide el estado de una tarea.
- **Determinismo:** lo único no determinista del núcleo es el sorteo de las
  esperas (D09.3). Cada trabajo tiene ahora su propio generador, sembrado con su
  identificador. Al repetir las llamadas salen los mismos números en el mismo
  orden, y el historial queda idéntico, `retry_in_s` incluido. La
  especificación se guarda como el documento JSON que acepta `parse_job`, y se
  vuelve a validar al leerla.
- **Consecuencia:** al arrancar se repite el diario entero, también el de los
  trabajos terminados. El coste crece con el diario, porque no se compacta. Con
  muchos trabajos habría que guardar la vista final de los terminados; queda
  fuera de alcance.

### D14.2 — Primero el diario, después el efecto

- **Decisión:** cada llamada se escribe en el diario antes de hacerla, y por
  tanto antes de cualquier efecto que salga del actor: la respuesta a un envío,
  los intentos que desbloquea y la llamada que lanza un intento. Cada intento
  deja su `running` justo antes de lanzarse, también el primero de cada tarea.
- **Porqué:** tras una caída, todo lo que el exterior vio está explicado en el
  diario. Si el cliente recibió un identificador, el trabajo está en el diario y
  reenviarlo con su clave lo encuentra. Si un intento salió hacia un trabajador,
  su `running` está en el diario. SQLite en modo autocommit deja cada inserción
  en disco al volver, igual que en D12.1.
- **Coste:** una escritura síncrona por llamada. Un trabajo de *n* tareas sin
  fallos escribe 1 + 2*n* filas.
- **Fuera del modelo:** un fallo del propio disco (02 §1).

### D14.3 — Ray reinicia el actor y el constructor repite el diario

- **Opciones:**
  - (a) que Ray reinicie el actor (`max_restarts`);
  - (b) que la pasarela lo vuelva a crear cuando no responda;
  - (c) un supervisor propio.
- **Decisión:** (a), sin límite de reinicios. Al arrancar, el constructor
  repite el diario. Si quedan intentos sin resolver, se llama a sí mismo
  (`recover`) para ocuparse de ellos en cuanto esté en marcha.
- **Porqué:** Ray ya detecta la muerte del proceso del actor y lo vuelve a
  crear en el mismo nodo, con los mismos argumentos. Eso es sustrato, como la
  colocación (D02.1). Lo propio es qué estado hay y cómo se reconstruye. (b) y
  (c) repetirían esa detección.
- **`max_task_retries` sigue en 0:** Ray no reenvía las llamadas que estaban en
  curso cuando el actor murió. Reenviar un envío solo es seguro con su clave de
  idempotencia, así que eso lo hace el cliente (D12.7, D14.5).
- **Si la repetición falla**, por un error de programación, el constructor
  lanza la excepción. Ray no reinicia un actor cuyo constructor falla, y la
  pasarela contesta 503: el fallo se ve.

### D14.4 — Los intentos que quedaron en vuelo

- **Contexto:** tras repetir el diario, la máquina de estados sabe qué intentos
  repartió que no se resolvieron (`Job.unsettled()`). Su respuesta ya no puede
  llegar, porque quien la esperaba era el proceso muerto.
- **Decisión:**
  - **Sin `running` en el diario:** el intento no llegó a lanzarse. Se lanza
    ahora y no cuenta como fallo.
  - **Con `running`:** pudo ejecutarse. Se reconcilia (D12.4): si el almacén
    tiene su resultado, se adopta. Si no lo tiene, es un fallo `WORKER` con el
    motivo `coordinator restart` y gasta presupuesto según D09.2.
- **Porqué gasta presupuesto:** a una tarea con los reintentos de
  infraestructura a 0 se le promete «como máximo una vez» (02 §1). Volver a
  lanzar un intento que pudo ejecutarse rompería esa promesa. Con el presupuesto
  por defecto, tres reintentos de trabajador, no se nota.
- **Porqué `WORKER`:** la evidencia es la muerte del proceso dueño del intento,
  y Ray detiene las tareas cuyo dueño muere. Se reintenta sin espera, como todo
  `WORKER` (D02.11). La correspondencia vive en el clasificador puro: la
  evidencia `RESTARTED` es `WORKER`.
- **Opciones descartadas:**
  - relanzarlo todo sin contarlo: rompe la promesa anterior;
  - darlo todo por fallido: castiga también a los que no llegaron a salir;
  - una cuarta clase de fallo: R6 pide tres, y lo que decide una clase, la
    política de reintento, es la misma que la de un trabajador caído.
- **Imprecisión conocida:** un intento cuyo `running` se escribió pero que no
  llegó a lanzarse cuenta como fallo. Es la ventana entre la escritura y la
  llamada a Ray, y se queda del lado seguro.
- **Un intento antiguo que siguiera vivo** y guardara después no cambia nada: el
  nuevo recibe ese resultado al guardar el suyo (regla 5 de D02.10), y S1 se
  mantiene.
- **Un reintento que esperaba** se lanza ya, sin la espera que le quedaba: el
  reinicio ya lo separó en el tiempo del fallo que lo causó.

### D14.5 — El cliente atraviesa un coordinador que no contesta

- **Contexto:** mientras el coordinador se reinicia, la pasarela contesta 503
  («coordinator unavailable»). Para un envío, un 503 es tan ambiguo como no
  recibir respuesta: el trabajo pudo quedar ya en el diario.
- **Decisión:** el cliente trata un 503 igual que la falta de respuesta.
  `submit` reintenta con la misma clave (D12.7), y `wait` sigue preguntando
  hasta 60 s seguidos sin respuesta antes de rendirse.
- **Porqué:** sin esto, un reinicio a mitad de un envío llevaría a reenviar a
  mano con otra clave y a duplicar el trabajo, justo lo que F1.6 evita. Y un
  `wait` en curso daría un error para un trabajo que va a terminar.
- **Límite:** `submit` mantiene sus tres intentos separados por 1 s (D12.7). Un
  reinicio más largo los agota, y el cliente lo dice con el código 3.

### D14.6 — Qué se garantiza ahora

- **L1** (todo trabajo aceptado termina) se cumple aunque el coordinador se
  reinicie, siempre que acabe en marcha. Desaparece la excepción de D07.8.
- **S1, S2 y S3 no cambian:** el almacén sigue siendo quien confirma, y la
  máquina de estados reconstruida aplica las mismas reglas.
- **Las claves de idempotencia sobreviven al reinicio**, que era el límite de
  D12.7.
- **El comando se sigue ejecutando al menos una vez.** Un reinicio puede añadir
  una ejecución, y esa ejecución se cuenta en el presupuesto.
- **No cubre:**
  - perder el disco del nodo de control (02 §1);
  - reiniciar solo el contenedor del nodo de control: los trabajadores no
    vuelven solos al clúster nuevo, y eso es plataforma (persona B). Si se
    recrea el clúster entero, el diario sigue en el volumen y el coordinador
    nuevo lo repite: así arrancó el de la segunda pasada de las pruebas, con
    los 22 trabajos de la primera;
  - el reinicio del actor del almacén, que sigue con `max_restarts=0`. Su
    estado ya está en disco, pero es pieza de la pista B.

## 3. Pruebas

| Nivel | Fichero | Qué comprueba |
|-------|---------|---------------|
| Unitarias | `tests/unit/test_store.py` | Cada tipo de llamada sale del diario tal como entró, en orden y también tras reabrirlo; la especificación vuelve igual (D14.1) |
| Unitarias | `tests/unit/test_scheduler.py` | `unsettled()` lista lo repartido sin resolver; repetir las mismas llamadas da el mismo trabajo, esperas incluidas (D14.1, D14.4) |
| Unitarias | `tests/unit/test_failures.py` | `RESTARTED` es `WORKER` (D14.4) |
| Unitarias | `tests/unit/test_gateway.py` | Un 503 es falta de respuesta; `submit` reintenta con la misma clave; `wait` sigue tras un 503 y se rinde pasado el margen (D14.5) |
| Integración | `tests/integration/test_coordinator.py` | Tras matar el actor: un resultado guardado se adopta sin repetir; un intento sin resultado es `WORKER` y se reintenta; sin presupuesto no se repite; los trabajos terminados y sus claves siguen ahí |
| Extremo a extremo | `tests/e2e/test_restart.py` | **Corte C8, parte F7.7:** con el cliente real esperando, se mata el coordinador a mitad de una tarea y el trabajo termina `SUCCEEDED` |

Estado el 2026-11-16 en el clúster local, todo en verde: 215 unitarias en el
contenedor (175 en Windows), 21 de integración, 23 de extremo a extremo y 8
desde el anfitrión.

**Lo que se vio en el corte**, en la traza del trabajo de `test_restart`
(`docker logs dpes-head`):

- El coordinador volvió en menos de medio segundo, en otro proceso, y repitió
  el diario: 20 trabajos, uno sin terminar.
- El intento `build/0/1` quedó huérfano con `launched=True`. Contó como
  `WORKER` con el motivo «coordinator restart» y se relanzó al instante como
  `build/0/2`, que terminó bien.
- `build/0/1` nunca registró el final de su comando: Ray lo detuvo al morir su
  dueño. Queda comprobada la hipótesis de D14.4.
- La pasarela contestó un único 503, con el motivo que da Ray: «The actor was
  restarted. The task may or may not have been executed». Es la misma
  ambigüedad que D14.5 trata, y el `wait` del cliente la atravesó.
- Ray avisa de que, si los argumentos del constructor viven en su almacén de
  objetos y se pierden, el reinicio falla
  ([ray-project/ray#53727](https://github.com/ray-project/ray/issues/53727)).
  El aviso sale en la prueba de integración, donde el ejecutor sustituto viaja
  por valor; el coordinador que crea la pasarela no lo da.
