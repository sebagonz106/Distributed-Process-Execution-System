# 09 — Fase 5: clasificación de fallos y reintentos

- **Fecha:** 2026-11-15
- **Estado:** cerrada el 2026-11-15. El corte C5 pasó en la CI de GitHub
  (ejecución 43740571857, commit `4406682`), incluida la caída real de un
  trabajador. R6 y R8 quedan cumplidos.
- **Paso:** Fase 5 de [03-fases-y-dependencias.md](03-fases-y-dependencias.md),
  adelantada a la Fase 3: solo dependía de la 2, ya cerrada
- **Requisitos:** R6 (clasificación de fallos de tarea, trabajador y red) y R8
  (políticas de reintento según el tipo de fallo)
- **Corte:** C5 — cuatro situaciones, cada una con su evidencia: comando con
  error → `TASK`, sin reintento, trabajo `FAILED`; trabajador muerto a mitad →
  `WORKER`, relanzado en otro nodo, trabajo `SUCCEEDED`; corte de red → `NETWORK`,
  esperas crecientes; presupuesto agotado → trabajo `ERROR`.

## 1. Alcance

| Feature | Qué | Estado |
|---------|-----|--------|
| F6.1 | Taxonomía de tres clases con su evidencia | Hecho |
| F6.2 | Clasificador: función pura de la evidencia a la clase | Hecho |
| F6.3 | El estado final distingue fallo de la tarea de fallo de infraestructura | Hecho en la Fase 1 (D07.4); se mantiene |
| F6.4 | Inyección reproducible de un fallo de cada clase | Hecho |
| F6.5 | Plazo del coordinador por intento | Hecho; cierra D07.13 |
| F8.1 | Política por clase | Hecho |
| F8.2 | Presupuesto por clase, configurable por tarea | Hecho |
| F8.3 | Agotado el presupuesto, fallo definitivo que conserva la clase | Hecho |
| F8.4 | Evitar el nodo sospechoso (C) | No se hace (D09.9) |

## 2. Decisiones

### D09.1 — El clasificador es una función pura de lo observado

| Lo que observa el coordinador | Clase |
|-------------------------------|-------|
| `TaskResult` que no es un éxito | `TASK` |
| `ExecutorFailure` con `WORKSPACE_FAILED` | `WORKER` |
| `ExecutorFailure` con cualquier otra evidencia (entradas, digest, subida, guardado) | `NETWORK` |
| Vence el plazo propio del intento (D09.4) | `NETWORK` |
| Cualquier excepción de Ray (`WorkerCrashedError`, `NodeDiedError`, otras) | `WORKER` |

- Es la tabla de D06.14 llevada a código, en `dpes/failures.py`, sin Ray: recibe
  el resultado, el fallo informado o el nombre de la excepción.
- **Una excepción inesperada cuenta como `WORKER`:** es infraestructura (no
  hay resultado del comando) y se reintenta de inmediato. Si el problema es un
  error de programación del ejecutor, se repetirá y el presupuesto, que es
  finito, lo convertirá en `ERROR`. No se añade una cuarta clase porque la
  orientación pide tres.
- **El coordinador clasifica; la máquina de estados recibe la clase.** Así la
  máquina de estados no sabe nada de Ray ni de evidencias, solo de clases y
  presupuestos.

### D09.2 — Política por clase, con presupuesto por tarea

| Clase | Reintentos por defecto | Espera antes de reintentar |
|-------|:----------------------:|----------------------------|
| `TASK` | 0 | Ninguna |
| `WORKER` | 3 | Ninguna |
| `NETWORK` | 4 | Exponencial con aleatoriedad, entre 0,5 y 30 s (D09.3) |

- Es D02.11. Cada tarea puede cambiar sus presupuestos con
  `"retries": {"task": 2, "worker": 0, "network": 1}`, cada uno un entero entre
  0 y 10 (F8.2). Poner a cero los de infraestructura da «como máximo una vez»
  para tareas con efectos fuera del espacio de trabajo (02 §1).
- **Los presupuestos son por clase, no uno global:** que la red falle dos veces
  no gasta los reintentos que protegen de la caída de un trabajador.
- **Agotado el presupuesto**, la tarea termina en `FAILED` si la clase es
  `TASK` y en `ERROR` si es de infraestructura (F8.3), con la clase del último
  fallo a la vista.
- **Un reintento de `TASK` abre ronda nueva; uno de infraestructura, no.** Es
  la regla de la clave de resultado de D02.10, que se usará en la Fase 6.

### D09.3 — Espera exponencial con aleatoriedad «a medias»

- **Decisión:** antes del reintento *n* (desde 0) de clase `NETWORK` se espera
  un valor uniforme entre la mitad y el total de `min(30, 1·2ⁿ)` segundos.
- **Opciones:** sin aleatoriedad; aleatoriedad completa (uniforme entre 0 y el
  tope); aleatoriedad a medias.
- **Porqué:** sin aleatoriedad, las tareas que fallaron juntas por el mismo
  corte reintentan juntas y vuelven a cargar la red a la vez (Lab4). Con
  aleatoriedad completa, una espera puede salir más corta que la anterior. A
  medias, cada espera cae en `[tope/2, tope]`; mientras el tope se duplica,
  cada espera es al menos tan larga como la anterior, así que las esperas
  crecen, como pide C5, y siguen desincronizadas.
- **Hasta dónde vale la garantía:** solo mientras el tope se duplica, es decir,
  hasta el quinto reintento (topes de 1, 2, 4, 8 y 16 s). Cubre el presupuesto
  por defecto de cuatro reintentos. A partir del tope de 30 s las esperas
  quedan entre 15 y 30 s y una puede salir más corta que la anterior. La
  primera versión de este texto prometía crecimiento sin límite; la prueba
  unitaria lo desmintió y se corrigió el texto, no el diseño.
- **Determinismo:** el azar se inyecta en la máquina de estados, que sigue
  siendo pura; las pruebas pasan un valor fijo.

### D09.4 — Plazo por intento que empieza cuando el intento arranca

- **Problema:** el experimento mostró que el plazo propio es la única señal de
  un trabajador pausado o incomunicado (D06.14). Pero un intento puede pasar
  tiempo en la cola de Ray esperando recursos (D08.4). Si el plazo contara
  desde el lanzamiento, un intento encolado vencería sin haber empezado, se
  reintentaría, volvería a la cola, y así hasta agotar el presupuesto: un
  clúster cargado convertiría tareas sanas en `ERROR`.
- **Decisión:** cada intento se lanza envuelto en una tarea de Ray que, antes
  de ejecutar el comando, avisa al coordinador de que arrancó. Hay dos plazos:
  - **de cola:** desde el lanzamiento hasta el aviso de arranque, 1 hora;
  - **de ejecución:** desde el aviso, el tiempo límite de la tarea más 5 s de
    margen. El espacio aislado mata el comando al vencer su tiempo límite
    (D07.15), así que pasado el margen sin noticias solo puede ser un fallo de
    infraestructura.
- **Al vencer cualquiera de los dos**, el coordinador cancela el intento en
  Ray (forzado, si aún vive) y lo clasifica `NETWORK`.
- **Porqué el plazo de cola:** el aviso de arranque es un mensaje y se puede
  perder. Sin un límite, un intento cuyo aviso se perdió y cuyo trabajador quedó
  incomunicado esperaría para siempre. Con él, L1 (todo trabajo termina) se
  cumple.
- **Precio:** un intento que de verdad espera recursos más de una hora se
  reintenta. Es aceptable para trabajos de compilación y pruebas.
- **Dónde:** el envoltorio y los plazos viven en `coordinator.py`; el ejecutor
  sigue sin conocer Ray (D07.17).

### D09.5 — `RETRY_WAIT` es observable

- **Decisión:** al decidir un reintento, la tarea pasa a `RETRY_WAIT` con el
  intento nuevo ya creado. Cuando el coordinador lo lanza tras la espera, la
  tarea vuelve a `RUNNING`. El diagrama de estados (05) ya lo dibujaba.
- **Porqué:** durante una espera de red de varios segundos, `RUNNING` diría
  algo falso.

### D09.6 — La evidencia se ve en el historial de intentos de cada tarea

- **Decisión:** la vista de cada tarea gana `attempts`: por intento, su
  identificador de ejecución, cómo terminó (`SUCCEEDED` o la clase de fallo),
  el motivo observado y la espera que se decidió antes del siguiente.
- **Porqué:** C5 pide ver la evidencia de cada fallo. El historial de eventos
  del trabajo (F5.5) es de la Fase 4, que aún no está; el de intentos es del
  coordinador y basta para C5. Cuando llegue el historial de eventos, cada
  entrada de `attempts` será también un evento.

### D09.7 — Fallos inyectados desde dentro, solo si el clúster lo permite

- **Decisión:** si el trabajador tiene `DPES_FAULT_INJECTION=1`, una tarea
  puede pedir que fallen sus primeros *n* intentos con la variable
  `DPES_FAULT=<tipo>:<n>`:

  | Tipo | Qué hace el ejecutor | Lo que ve el coordinador |
  |------|----------------------|--------------------------|
  | `crash` | Termina su propio proceso | `WorkerCrashedError` → `WORKER` |
  | `network` | Informa `INPUT_UNAVAILABLE` | `ExecutorFailure` → `NETWORK` |
  | `hang` | Se queda colgado sin ejecutar nada | Vence el plazo → `NETWORK` |

- **Porqué desde dentro:** es D02.15. Hace reproducibles, en la CI, fallos cuyo
  instante desde fuera dependería de acertar milisegundos.
- **Porqué el interruptor en el trabajador:** sin él, cualquier trabajo podría
  tumbar procesos del clúster a voluntad. La composición de este proyecto lo
  activa porque es el clúster de pruebas y demostración; se dice en ella.
- **Desde fuera también:** la caída real de un contenedor se prueba con un
  guion que corre en el anfitrión y usa Docker (`tests/chaos`), igual que el
  experimento de la Fase 0.

### D09.8 — Se cierra D07.13

El plazo por intento existe: un trabajador pausado o incomunicado ya no deja un
trabajo en `RUNNING` para siempre. L1 se cumple también ante esos fallos.

### D09.9 — F8.4 (evitar el nodo sospechoso) no se hace

- **Consecuencia que se acepta:** si un trabajador está incomunicado pero Ray
  aún no lo dio por muerto (D06.14), Ray puede volver a colocar el reintento en
  él. Ese reintento también vencerá y el presupuesto de `NETWORK` acabará en
  `ERROR`. Es una C y la primera en el orden de recorte (D03.6).

### D09.10 — El plazo vencido no se confunde con lo que devuelve el ejecutor

- **Contexto:** la primera versión del coordinador representaba igual, como
  una cadena, el nombre de una excepción de Ray y lo que fuera que devolviera
  el ejecutor. Un ejecutor que devolvía una cadena por error se tomaba por una
  excepción, con esa cadena como motivo. La prueba de integración del
  ejecutor que rompe su contrato lo encontró.
- **Decisión:** el plazo vencido se señala con una excepción privada del
  coordinador; las excepciones de Ray se registran por el nombre de su clase;
  y cualquier valor devuelto que no sea `TaskResult` ni `ExecutorFailure` se
  registra como «executor returned <tipo>». Cada fuente de evidencia tiene su
  propio camino (D06.3).

## 3. Pruebas

| Nivel | Fichero | Qué comprueba |
|-------|---------|---------------|
| Unitarias | `tests/unit/test_failures.py` | La tabla de D09.1 fila a fila, y las esperas de D09.3: entre la mitad y el tope, crecientes mientras el tope se duplica, entre 15 y 30 s después |
| Unitarias | `tests/unit/test_spec.py` | `retries`: valores por defecto, sustitución por clase y límites |
| Unitarias | `tests/unit/test_scheduler.py` | Ronda nueva solo para `TASK`, reintento inmediato de `WORKER`, esperas crecientes de `NETWORK`, presupuestos independientes, `RETRY_WAIT`, historial de intentos, resultado tardío ignorado y dependientes que esperan al reintento |
| Unitarias (Linux) | `tests/unit/test_executor.py` | Cada fallo inyectable, que solo actúa con el interruptor y que un valor mal formado se ignora |
| Integración | `tests/integration/test_coordinator.py` | Trabajador que muere una vez, intento colgado que agota el plazo de ejecución, intento atascado en la cola que agota el de cola, y ejecutor que rompe su contrato |
| Extremo a extremo | `tests/e2e/test_failures.py` | **Corte C5** por la pasarela, con fallos inyectados: `TASK`, `WORKER`, `NETWORK` con esperas crecientes, plazo vencido y presupuesto agotado |
| Caída real | `tests/chaos/test_worker_loss.py` | **Corte C5** desde fuera: se mata con Docker el contenedor que ejecuta la tarea; el fallo es `WORKER` y el reintento termina en el otro trabajador |

Estado el 2026-11-15, todo en verde: 125 unitarias en el contenedor (106 en
Windows), 12 de integración, 16 de extremo a extremo y la de caída real,
ejecutada desde Windows. La CI ejecuta la de caída real en la máquina de
GitHub, fuera de los contenedores.

Dos pruebas detectaron errores de diseño antes de que llegaran al código
final: la de las esperas (D09.3, la garantía de crecimiento tenía un límite) y
la del contrato del ejecutor (D09.10). Una tercera, la del fallo `hang`,
pasaba sin que el código existiera porque interceptaba una espera que también
usa `subprocess`; se corrigió para que interceptara solo la del ejecutor.

Para la de caída real, con el clúster levantado, desde la raíz del repositorio:

```
py -m unittest discover -s tests/chaos
```
