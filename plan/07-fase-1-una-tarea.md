# 07 — Fase 1: una tarea de extremo a extremo

- **Fecha:** 2026-11-15
- **Estado:** cerrada el 2026-11-15. El corte C1 pasó en la CI de GitHub
  (ejecución 43732301774, commit `5f75c79`). Con el cierre quedan congelados
  los contratos de `dpes/messages.py` (D03.3), con el campo `output_tail` de
  D07.14; la persona B no llegó a revisarlos.
- **Paso:** Fase 1 de [03-fases-y-dependencias.md](03-fases-y-dependencias.md)
- **Corte:** C1 — se envía un trabajo de una tarea y `status` muestra
  `SUCCEEDED` con su salida; con un comando que falla muestra `FAILED` y el
  código; con uno que excede su tiempo límite, el árbol de procesos entero
  desaparece.

## 1. Alcance y estado

| Parte | Features | Quién | Estado |
|-------|----------|:-----:|--------|
| Especificación de una tarea y su validación | F1.1 (una tarea) | A | Hecho |
| Máquina de estados mínima | F2.5 (mínima) | A | Hecho |
| Pasarela HTTP: enviar y consultar | F1.3 | A | Hecho |
| Cliente: `submit`, `status`, `wait` | F1.4 | A | Hecho |
| Coordinador como actor de Ray | — | A | Hecho, probado en el clúster con un ejecutor de prueba |
| Espacio aislado: directorio, entorno, grupo de procesos, tiempo límite | F3.1, F3.3, F3.4 | B, hecho por A | Hecho |
| Ejecutor como tarea de Ray y captura de la salida | F3.5, F5.1 | B, hecho por A | Hecho |
| Pasarela en el clúster y prueba C1 | — | A | Hecho; C1 en verde en la CI |

La parte de la persona B la hizo la persona A el 2026-11-15, a petición
propia, como en la Fase 0.

## 2. Decisiones

### D07.1 — Formato del trabajo en la Fase 1

```json
{
  "tasks": {
    "hola": {
      "command": ["sh", "-c", "echo hola $NOMBRE"],
      "env": {"NOMBRE": "mundo"},
      "timeout_s": 60
    }
  }
}
```

| Campo | Regla | Si falta |
|-------|-------|----------|
| `tasks` | Objeto nombre → tarea. Nombre de 1 a 64 caracteres de `[A-Za-z0-9_.-]` | Error |
| `command` | Lista no vacía de cadenas; la primera no vacía (D01.3) | Error |
| `env` | Objeto de cadena a cadena; nombres `[A-Za-z_][A-Za-z0-9_]*` | `{}` |
| `timeout_s` | Número mayor que 0 y como mucho 86 400 | 600 |

- **Nombres restringidos:** el nombre de una tarea aparecerá en rutas de la API
  (D02.6) y en nombres de ficheros de logs; con ese alfabeto no hay que
  escaparlo en ningún sitio.
- **Tiempo límite con valor por defecto:** toda tarea tiene que tenerlo, porque
  es lo que garantiza que un intento termina (L1) y, tras el experimento, lo que
  permitirá fijar el plazo del coordinador (D06.14). Un valor por defecto es más
  cómodo que exigirlo y no lo debilita. 600 s cubre una compilación o unas
  pruebas pequeñas; quien necesite más lo declara.
- **Un booleano no es un número:** en Python `True` es un entero; se rechaza
  explícitamente para que `"timeout_s": true` no se lea como 1 segundo.

### D07.2 — La validación rechaza lo desconocido y devuelve todos los errores

- **Decisión:** un campo que el formato no conoce es un error, en el trabajo y
  en cada tarea. La validación no se detiene en el primer error: devuelve la
  lista entera, cada uno con la ruta del campo (`tasks.hola.timeout_s`).
- **Porqué:** es la frontera de confianza (F1.2). Aceptar un campo desconocido
  en silencio hace que una errata (`"timout_s"`) pase a usar el valor por
  defecto sin que nadie se entere. Devolver todos los errores ahorra al cliente
  un envío por error.
- **Consecuencia:** los campos de fases posteriores (`needs`, `inputs`,
  `outputs`, recursos, reintentos, caché) se rechazan hasta que la fase que los
  implementa los añada. Un trabajo nunca se acepta prometiendo algo que el
  sistema todavía no hace.

### D07.3 — En la Fase 1 un trabajo tiene exactamente una tarea

- **Decisión:** la validación rechaza un trabajo con cero o con más de una
  tarea. La Fase 2 levanta la restricción al añadir dependencias.
- **Porqué:** sin dependencias, varias tareas serían varias tareas
  independientes, y el comportamiento al fallar una (D01.4) solo tiene sentido
  con el grafo. Aceptarlas ahora sería decidir eso a medias.
- **Consecuencia:** el formato ya es el definitivo (un mapa de tareas); la
  restricción es una regla de validación que se borra en la Fase 2.

### D07.4 — Estados de la Fase 1 y estado del trabajo derivado

- **Tarea:** `PENDING` → `RUNNING` → `SUCCEEDED`, `FAILED` o `ERROR`.
  - `SUCCEEDED`: el resultado es un éxito (D06.2).
  - `FAILED`: el comando terminó y su resultado no es un éxito.
  - `ERROR`: no hay resultado del comando (excepción de Ray o fallo informado
    por el ejecutor).
- **Trabajo:** se deriva de sus tareas, nunca se guarda aparte:
  `PENDING` si ninguna arrancó, `RUNNING` si alguna no terminó, y al terminar
  todas: `FAILED` si alguna es `FAILED`, si no `ERROR` si alguna es `ERROR`, si
  no `SUCCEEDED` (D01.5).
- **Porqué derivado:** si el estado del trabajo se guardara aparte podría
  contradecir al de sus tareas; derivado, no puede.
- **Lo que falta a propósito:**
  - `READY` y `SKIPPED` (D05.3) llegan con las dependencias en la Fase 2.
  - `RETRY_WAIT` y la clasificación de fallos llegan en la Fase 5. Hasta
    entonces todo fallo sin resultado es `ERROR` y no se reintenta: es la
    política de D02.11 con presupuesto cero.

### D07.5 — Identificador de ejecución determinista

- **Decisión:** el identificador de un intento es
  `<trabajo>/<tarea>/<ronda>/<intento>`, donde el intento cuenta desde 1 dentro
  del trabajo y la tarea. El identificador del trabajo es un UUID aleatorio que
  pone el coordinador.
- **Porqué:** la máquina de estados no necesita ni reloj ni azar, así que sus
  pruebas son deterministas (D02.3). El identificador se lee en un log y dice a
  qué intento de qué tarea pertenece (Lab4: el mismo identificador sigue a la
  operación por todos los procesos). Es único porque el del trabajo lo es.

### D07.6 — Un evento de un intento desconocido o ya resuelto se ignora

- **Decisión:** si llega un resultado o un fallo para un identificador de
  ejecución que no es el intento en curso de su tarea, la máquina de estados no
  cambia nada.
- **Porqué:** es la regla 6 de D02.10 y la consecuencia 4 del experimento
  (D06.14): resultados duplicados, tardíos o de intentos abandonados no pueden
  mover el estado. Se implementa desde la primera versión para que el resto de
  fases no tenga que añadirla.

### D07.7 — Coordinador: actor asíncrono fijado al nodo de control

- **Decisión:** un actor de Ray con nombre (`coordinator`, espacio de nombres
  `dpes`), `num_cpus=0`, fijado al nodo de control y sin reinicios. Es
  asíncrono: por cada intento lanza el ejecutor como tarea de Ray con
  `max_retries=0` (D02.2) y espera su resultado sin bloquear al resto.
- **Porqué asíncrono:** un actor procesa un mensaje a la vez (D02.3). Si
  esperara un intento de forma bloqueante, ninguna consulta de estado se
  atendería mientras corre una tarea. Con un actor asíncrono, la espera cede el
  turno y el estado sigue teniendo un solo dueño.
- **Porqué fijado:** D06.8. Con `num_cpus=0` Ray podría colocarlo en un
  trabajador; se fija al nodo donde arranca la pasarela, que es el de control.
- **Ejecutor inyectado:** el actor recibe la función del ejecutor al crearse.
  En producción es la de `dpes/executor.py` (pista B); en las pruebas, una de
  prueba. Así el coordinador se prueba en el clúster sin esperar al ejecutor.

### D07.8 — El estado del coordinador vive en memoria hasta la Fase 8

- **Contexto:** D02.4 decide que el estado sea durable en SQLite, y el
  calendario (D04) ponía «estado en disco» en esta fase.
- **Decisión:** en la Fase 1 el estado del coordinador vive en la memoria del
  actor. La escritura en disco se hace en la fase que prueba la recuperación del
  coordinador (F7.7, Fase 8). **No cambia D02.4, solo cuándo se implementa.**
- **Porqué:** el disco solo importa si el coordinador se reinicia, y eso no lo
  ejercita ninguna prueba hasta la Fase 8. Escribirlo ahora sería código sin
  prueba que lo justifique (D06.6). Los resultados durables que necesita la
  confirmación perdida son del almacén (D02.10), no del coordinador.
- **Consecuencia aceptada hasta la Fase 8:** si el actor muere, se pierden los
  trabajos en curso. Si F7.7 se recorta por la línea de corte (D03.6), D02.4 se
  cumple solo para el almacén, y así se dirá en la entrega.

### D07.9 — Pasarela sobre `http.server` y API mínima

| Verbo y ruta | Éxito | Errores |
|--------------|-------|---------|
| `POST /jobs` | `201 {"job_id": …}` | `400 {"errors": […]}` si no valida; `413` si el cuerpo pasa de 1 MB |
| `GET /jobs/{id}` | `200` con el estado del trabajo y de sus tareas | `404` |

- Cualquier otra ruta da `404`. Si el coordinador no responde, `503`.
- **Valida la pasarela:** el coordinador recibe siempre un trabajo ya validado
  (D02.6). El cuerpo se limita a 1 MB antes de leerlo, para que un cliente no
  agote la memoria del nodo de control.
- **La pasarela no conoce Ray:** recibe un objeto con dos métodos, `submit` y
  `status`. En producción es un adaptador al actor; en las pruebas, uno en
  memoria. Así la API se prueba en Windows, sin clúster.

### D07.10 — El cliente sale con un código distinto por cada final

- **Decisión:** `python -m dpes.cli wait <id>` espera a que el trabajo termine y
  sale con `0` si es `SUCCEEDED`, `1` si es `FAILED` y `2` si es `ERROR`. La
  dirección de la pasarela se toma de `--url`, de la variable `DPES_URL` o, por
  defecto, `http://localhost:8000`.
- **Porqué:** quien automatiza (un guion, otra CI) decide con el código de
  salida, sin leer JSON. Que fallo de la tarea y fallo de infraestructura den
  códigos distintos es la distinción de la orientación llevada hasta el último
  eslabón: «tu código está roto» frente a «reenvía».
- **Espera por consulta periódica:** una consulta por segundo. Es la opción más
  simple con la API de D07.9; la transmisión en vivo llega con los logs
  (Fase 4).

### D07.11 — `PYTHONPATH=/app` en la imagen

- **Contexto:** los procesos que Ray arranca en cada nodo no tienen `/app` en
  su ruta de búsqueda, así que no podrían importar `dpes` para deserializar un
  intento o ejecutar el actor.
- **Decisión:** la imagen fija `PYTHONPATH=/app`. Todos los nodos usan la misma
  imagen (D06.7), así que el código es el mismo en todos.
- **Alternativa descartada:** el entorno de ejecución de Ray que copia el
  código del driver a cada nodo: sube el paquete en cada arranque y hace que
  dos nodos puedan ejecutar versiones distintas.

### D07.12 — Contrato del ejecutor que espera el coordinador

> **Precisada por D07.17:** el ejecutor es una función normal, `execute`; el
> coordinador la convierte en tarea de Ray.

- **Decisión:** `dpes/executor.py` (pista B) expone `run_attempt`, una función
  remota de Ray que recibe un `Attempt` y **devuelve** un `TaskResult` o un
  `ExecutorFailure` (D06.3). El coordinador la lanza con `max_retries=0`.
- **Defensa en el coordinador:** cualquier otra cosa que llegue (un tipo
  distinto, una excepción al lanzar el intento) se registra como `ERROR` con el
  motivo. Ningún camino deja la tarea en `RUNNING`.
- **Porqué:** la espera de cada intento corre en una tarea de fondo del actor.
  Una excepción no capturada ahí no la ve nadie: el trabajo quedaría en
  `RUNNING` para siempre. La revisión de esta fase lo encontró, y la prueba de
  integración que lo reproduce falló antes de corregirlo.

### D07.13 — Límite conocido hasta la Fase 5: sin plazo por intento

> **Resuelto por D09.4 y D09.8** (Fase 5, 2026-11-15): cada intento tiene un
> plazo de cola y otro de ejecución.

- **Hecho:** el coordinador todavía no pone plazo a un intento. Con un
  trabajador pausado o incomunicado, Ray no avisa (D06.14), así que en esta fase
  el trabajo se queda en `RUNNING` hasta que el trabajador vuelve.
- **Porqué se acepta ahora:** el plazo es F6.5, de la Fase 5, junto con la
  clasificación que decide qué hacer al vencer. Añadirlo ahora sin
  clasificación sería decidir a medias la política de reintentos.
- **Riesgo:** L1 (todo trabajo termina) no se cumple ante esos fallos hasta la
  Fase 5.

### D07.14 — La salida de la tarea viaja en el resultado, acotada

- **Contexto:** C1 pide que `status` muestre la salida. La transmisión de logs
  en vivo es de la Fase 4 (D02.8).
- **Decisión:** `TaskResult` gana el campo `output_tail`: los últimos 64 KiB de
  la salida estándar y de error, juntas y en orden, decodificados como UTF-8.
  Si se recortó, empieza con una línea que dice cuántos bytes se omitieron. La
  vista del trabajo lo muestra como `output`.
- **Porqué:** es lo mínimo que cumple C1 sin adelantar la Fase 4. El límite
  impide que una tarea que escribe gigas los arrastre por el resultado, el
  coordinador y la respuesta HTTP. La Fase 4 añade el log completo y en vivo;
  la cola sigue siendo útil para ver de un vistazo por qué falló una tarea.
- **Contrato:** cambia `TaskResult` (D06.2). Se permite porque los contratos no
  se congelan hasta cerrar esta fase (D03.3).

### D07.15 — Espacio aislado: qué se aísla y cómo

| Propiedad (01, R3) | Mecanismo |
|--------------------|-----------|
| Directorio nuevo por intento (F3.1) | `tempfile.mkdtemp`, permisos `0700`, se borra siempre al terminar |
| Entorno limpio (F3.3) | Solo `PATH`, `LANG=C.UTF-8`, `HOME` (el propio espacio) y las variables declaradas, que pueden sobrescribirlas |
| Ningún proceso sobrevive (F3.4) | El comando arranca en una sesión propia; al terminar o al vencer el tiempo límite se manda `SIGKILL` a todo su grupo |
| Sin entrada | La entrada estándar es `/dev/null` |

- **Se mata el grupo también cuando el comando termina bien:** un
  `servidor &` dentro del comando seguiría vivo después del intento, ocupando el
  trabajador y fuera de cualquier tiempo límite (01, R3: «no puede dejar
  procesos vivos»).
- **La salida va a un fichero, no a una tubería:** un proceso en segundo plano
  hereda la tubería y la mantiene abierta; leer hasta el final esperaría a ese
  proceso y el intento no terminaría nunca. Con un fichero, la espera es solo
  del proceso principal.
- **Un comando que no existe es fallo de la tarea:** termina con código 127
  (126 si no tiene permiso de ejecución), como en un shell. Es un error del
  trabajo enviado, no de la infraestructura (D02.9), y reintentarlo daría lo
  mismo.
- **Límite:** un proceso que crea su propia sesión (`setsid`) sale del grupo y
  escapa. Cerrar ese hueco requiere espacios de nombres de procesos (F3.7).

### D07.16 — Cada intento corre con un usuario sin privilegios propio

- **Contexto:** dentro del contenedor todo corre como `root`, y un trabajador
  ejecuta dos intentos a la vez (D06.9, dos CPU). Como `root`, una tarea podría
  leer el espacio de trabajo de la otra, pese a sus permisos `0700`, y matar
  sus procesos. Eso contradice D02.5, que promete aislar las tareas entre sí.
- **Decisión:** si el ejecutor corre como `root`, el comando se ejecuta con el
  usuario y grupo numérico `100000 + pid del proceso ejecutor`, sin grupos
  adicionales, y el espacio de trabajo pasa a ser suyo.
- **Porqué ese número:** Ray ejecuta una tarea a la vez en cada proceso
  ejecutor, y dos procesos vivos del mismo contenedor no comparten pid. Así dos
  intentos simultáneos tienen siempre usuarios distintos sin coordinar nada
  entre ellos. No hace falta que el usuario exista en `/etc/passwd`.
- **Si no es `root`** (las pruebas en la CI fuera del contenedor), no se cambia
  de usuario: no se puede.
- **Límite:** la tarea sigue pudiendo leer los ficheros del sistema del
  trabajador, escribir en `/tmp` y usar la red. Es el límite ya escrito en
  D02.5; F3.7 lo cerraría.

### D07.17 — El ejecutor no conoce Ray

- **Decisión:** `dpes/executor.py` expone `execute(attempt)`, una función
  normal: crea el espacio de trabajo, ejecuta el comando en él y lo destruye.
  El coordinador la convierte en tarea de Ray (`ray.remote(execute)`) al
  arrancar. Si no puede crear el espacio, devuelve
  `ExecutorFailure(WORKSPACE_FAILED)`.
- **Porqué:** igual que la pasarela (D07.9): el ejecutor y el espacio aislado se
  prueban en cualquier Linux, también en la CI sin instalar Ray, y Ray queda en
  un solo módulo, el coordinador.

### D07.18 — La pasarela arranca con el nodo de control y solo escucha en local

- **Decisión:** el nodo de control arranca Ray y después la pasarela, en el
  mismo contenedor. Su puerto se publica en `127.0.0.1:8000` del anfitrión, el
  único puerto publicado del clúster (D06.10).
- **Porqué en local:** el servicio ejecuta comandos arbitrarios sin
  autenticación (D01.8). Publicarlo en todas las interfaces lo abriría a la red
  de la máquina; en `127.0.0.1` solo lo alcanza quien ya está en ella.

### D07.19 — Un proceso `init` en cada contenedor

- **Decisión:** `init: true` en todos los servicios de la composición.
- **Porqué:** al matar el grupo de una tarea, sus procesos huérfanos pasan al
  proceso 1 del contenedor, que es Ray o la pasarela y no los recoge. Quedarían
  como procesos zombis acumulándose en el trabajador. Con `init: true`, Docker
  pone como proceso 1 un `init` mínimo que los recoge.

## 3. Pruebas

| Nivel | Fichero | Qué comprueba |
|-------|---------|---------------|
| Unitarias | `tests/unit/test_spec.py` | Cada regla de D07.1–D07.3 |
| Unitarias | `tests/unit/test_scheduler.py` | Transiciones de D07.4, identificadores de D07.5, eventos ignorados de D07.6 |
| Unitarias | `tests/unit/test_gateway.py` | La API de D07.9 y el cliente de D07.10, contra un coordinador en memoria |
| Unitarias (Linux) | `tests/unit/test_sandbox.py` | Cada propiedad de D07.15; las dos de D07.16 solo como `root` |
| Unitarias (Linux) | `tests/unit/test_executor.py` | D07.17: resultado del intento, entorno, espacio nuevo y destruido, fallo al crear el espacio |
| Integración | `tests/integration/test_coordinator.py` | El actor en el clúster con un ejecutor de prueba: éxito, fallo del comando, muerte del proceso, ejecutor que rompe su contrato (D07.12) y consultas atendidas mientras corre un intento (D07.7) |
| Extremo a extremo | `tests/e2e/test_one_task.py` | **Corte C1** por la pasarela: salida de una tarea correcta, código de un comando que falla, tiempo límite, y que la tarea corre en un trabajador, sin `root` y sin el entorno del trabajador |

Estado el 2026-11-15, todo en verde:

| Dónde | Unitarias | Integración | Extremo a extremo |
|-------|:---------:|:-----------:|:-----------------:|
| Contenedor, como `root` | 69 | 6 | 6 |
| Contenedor, sin privilegios | 69 (2 saltadas) | — | — |
| Windows | 55 (2 módulos saltados: necesitan Linux) | — | — |

Las pruebas de D07.15 se repitieron cinco veces seguidas alternando `root` y
usuario sin privilegios. Una de ellas falló una vez de forma intermitente: el
auxiliar que comprueba si un proceso desapareció no contemplaba que el proceso
muriera entre abrir y leer `/proc/<pid>/stat`. Era un fallo de la prueba, no
del espacio aislado; corregido, las cinco rondas pasan.

La CI ejecuta, en este orden: unitarias fuera del contenedor (sin
privilegios), y dentro del clúster unitarias como `root`, integración y extremo
a extremo.

Para ejecutar las de integración, con el clúster levantado:

```
docker compose -f docker/compose.yaml exec -T head python -m unittest discover -s tests/integration
```
