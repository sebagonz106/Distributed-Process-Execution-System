# 06 — Fase 0: cimientos y experimento con Ray

- **Fecha:** 2026-11-15
- **Estado:** cerrada el 2026-11-15 en local; el corte C0 en la CI se
  confirma con el primer push
- **Paso:** Fase 0 de [03-fases-y-dependencias.md](03-fases-y-dependencias.md)
- **Corte:** C0

## 1. Alcance y estado

| Parte | Quién | Estado |
|-------|:-----:|--------|
| Estructura del paquete | A | Hecho |
| Contratos entre componentes | A | Propuestos; se congelan al cerrar la Fase 1 (D03.3) |
| Andamiaje de pruebas | A | Hecho |
| Imagen y clúster de contenedores | B, hecho por A | Hecho |
| Integración continua | B, hecho por A | Hecho |
| Experimento de fallos de Ray | A y B | Hecho (§3) |

La parte de plataforma, asignada a la persona B (D04.1), la hizo la persona A
el 2026-11-15 a petición propia. El reparto de las fases siguientes no cambia.

## 2. Decisiones

### D06.1 — Los contratos viven en un módulo propio, `messages.py`

- **Contexto:** D03.3 pide fijar como tipos los mensajes que cruzan entre
  coordinador, ejecutor, almacén y registro de logs. D02.16 no preveía dónde.
- **Opciones:** (a) cada tipo en el módulo de quien lo produce (el intento en
  el coordinador, el resultado en el ejecutor); (b) un módulo puro con todos.
- **Decisión:** (b), `dpes/messages.py`. **Amplía D02.16** con un módulo puro
  más; no cambia ninguno de los previstos.
- **Porqué:** con (a) el coordinador tendría que importar el módulo del
  ejecutor para conocer la forma del resultado, y al revés: las dos pistas
  quedarían acopladas y los módulos puros arrastrarían a los que usan Ray
  (rompe D02.3). Con (b) cada pista importa solo el contrato.
- **Consecuencia:** el diagrama de dependencias entre módulos de
  [05-diagramas.md](05-diagramas.md) no muestra `messages.py`. Se regenera al
  cerrar la Fase 1, cuando los contratos se congelan.

### D06.2 — Cinco mensajes

| Mensaje | De → a | Campos |
|---------|--------|--------|
| `Attempt` | Coordinador → ejecutor | trabajo, tarea, ronda, identificador de ejecución, comando, entorno, entradas (ruta → digest), salidas declaradas, tiempo límite |
| `TaskResult` | Ejecutor → almacén y coordinador | identificador de ejecución, código de salida, si venció el tiempo límite, salidas (ruta → digest), salidas declaradas que faltan |
| `ExecutorFailure` | Ejecutor → coordinador | identificador de ejecución, evidencia, detalle |
| `LogChunk` | Ejecutor → registro de logs | identificador de ejecución, número de secuencia, datos |
| `ResultKey` | Clave en el almacén | trabajo, tarea, ronda |

- **Porqué estos y no más:** son los cuatro que nombra D03.3 más la clave de
  resultado de D02.10, que comparten el coordinador (para consultar) y el
  ejecutor (para guardar). Las firmas de las llamadas entre componentes no se
  fijan aquí: dependen de decisiones de las Fases 1, 3 y 4.
- **Dos propiedades con lógica:**
  - `Attempt.result_key` es `(trabajo, tarea, ronda)`. No incluye el
    identificador de ejecución: así los reintentos de infraestructura de una
    misma ronda compiten por el mismo resultado, que es lo que sostiene S1
    (D02.10).
  - `TaskResult.succeeded` es cierto solo con código 0, sin tiempo límite
    vencido y sin salidas declaradas ausentes. Es la única definición de
    «éxito» del sistema; la usan el planificador (S2) y la caché (F9.3).
- **Lo que no lleva el intento:** los recursos pedidos. Sirven para que Ray
  elija el nodo, no para ejecutar; el ejecutor no los necesita.

### D06.3 — El ejecutor informa evidencia, no clase, y la devuelve como valor

- **Opciones:** (a) el ejecutor decide la clase del fallo y lanza una
  excepción; (b) el ejecutor devuelve `TaskResult` o `ExecutorFailure`, y este
  último solo dice *qué observó*.
- **Decisión:** (b). `Evidence` enumera lo que un ejecutor puede observar:
  no pudo preparar el espacio de trabajo, una entrada no llegó, un digest no
  coincide, no pudo subir una salida, no pudo guardar el resultado.
- **Porqué:** (1) clasificar es una función pura de la evidencia (F6.2) y vive
  en el coordinador; si el ejecutor clasificara, la taxonomía de D02.9 estaría
  repartida en dos sitios. (2) Devolver un valor deja las excepciones de Ray
  para lo que informa Ray (proceso o nodo muerto). Así las dos fuentes de
  evidencia no se mezclan: lo que dice el ejecutor llega como valor; lo que
  dice Ray, como excepción.

### D06.4 — Clases de datos congeladas de la biblioteca estándar

- **Decisión:** `dataclasses` con `frozen=True`. Viajan entre procesos con el
  mecanismo de serialización de Ray (pickle).
- **Porqué:** no requiere ninguna librería, da igualdad por valor (necesaria
  para comparar resultados en las pruebas de simulación de D02.3) e impide
  reasignar un campo de un mensaje ya enviado.
- **Límite:** los campos que son mapas (entorno, entradas, salidas) son
  diccionarios normales; `frozen` no impide modificar su contenido. Envolverlos
  en una vista de solo lectura los haría no serializables. Se acepta: un
  mensaje no se modifica después de crearlo, por convención.
- **Pendiente:** la forma en disco (SQLite) de estos mensajes se decide cuando
  exista quien los guarde (Fases 1 y 3).

### D06.5 — Pruebas unitarias en `tests/unit`, sin configuración

- **Decisión:** las pruebas puras viven en `tests/unit/test_*.py` y se ejecutan
  desde la raíz del repositorio con:

  ```
  py -m unittest discover -s tests/unit
  ```

  (`python` en lugar de `py` dentro de los contenedores). No hay fichero de
  configuración ni paquetes `__init__.py` en `tests/`.
- **Porqué:** al invocar `unittest` como módulo, Python añade el directorio
  actual a la ruta de búsqueda, de modo que `import dpes` funciona sin instalar
  el paquete ni tocar variables de entorno. Es el mismo comando en Windows, en
  el contenedor y en la integración continua.
- **Consecuencia:** cada nivel de D02.13 tendrá su directorio
  (`tests/integration`, `tests/e2e`) y su propia invocación.

### D06.6 — No se crean módulos vacíos

- **Decisión:** de la estructura de D02.16 solo existe lo que ya tiene
  contenido: `dpes/__init__.py` y `dpes/messages.py`. Cada módulo se crea en la
  fase que lo llena.
- **Porqué:** un fichero vacío no se puede probar ni revisar, y sugiere un
  avance que no existe.

### D06.7 — Imagen propia sobre `python:3.12-slim`

- **Opciones:** (a) la imagen oficial de Ray; (b) `python:3.12-slim` más
  `pip install ray==2.60.0`.
- **Decisión:** (b), con `gcc`, `libc6-dev` y `make` para los trabajos de
  demostración (D01.7). Ray fijado a una versión exacta en `requirements.txt`.
- **Porqué:** la imagen oficial pesa varios GB porque trae bibliotecas de
  aprendizaje automático que el proyecto no usa, y fija su propia versión de
  Python. Con (b) la imagen solo contiene lo que el sistema necesita, el Python
  es el que fija D02.15 y la versión de Ray no cambia sin un commit.
- **Una sola imagen** para el nodo de control y los trabajadores: lo que cambia
  es el comando de arranque, no el contenido. Se envía la recolección de
  estadísticas de uso de Ray desactivada.

### D06.8 — El nodo de control no ejecuta tareas

- **Decisión:** el nodo de control arranca con `--num-cpus=0`. Toda tarea va a
  un trabajador. La prueba de C0 lo comprueba.
- **Porqué:** el coordinador y el almacén viven en el nodo de control (D02.4).
  Si una tarea pudiera caer ahí, un comando que agota la memoria o se cuelga
  dejaría sin servicio a todo el sistema, y un fallo de tarea se convertiría en
  un fallo del plano de control. Separar los nodos separa los fallos.
- **Consecuencia:** los actores propios (coordinador, almacén, registro de
  logs) se crearán con `num_cpus=0` y fijados al nodo de control.

### D06.9 — Cada trabajador declara un recurso con su nombre

- **Decisión:** `worker-1` declara el recurso `{"worker-1": 1}` y `worker-2`
  el suyo. Una tarea que pida ese recurso solo puede ir a ese trabajador.
- **Porqué:** sin esto no se puede elegir dónde corre una tarea, y hacen falta
  dos casos: los experimentos de fallos (hay que saber qué contenedor romper) y
  la demostración de C3 (compilar en un trabajador y enlazar en otro para que la
  transferencia de artefactos sea real). Es el mecanismo de recursos con nombre
  que ya preveía D02.2.

### D06.10 — Ray sin autenticación, en una red sin puertos publicados

- **Contexto:** al arrancar, Ray avisa de que sin autenticación cualquiera con
  acceso de red al clúster puede ejecutar código arbitrario.
- **Decisión:** el clúster corre en su propia red de contenedores y la
  composición no publica ningún puerto en el anfitrión. No se activa la
  autenticación por token de Ray.
- **Porqué:** la autenticación está fuera de alcance (D01.8) y el sistema ya
  ejecuta comandos arbitrarios por diseño. Lo que se puede garantizar barato es
  que el clúster no sea alcanzable desde fuera de la red de contenedores.
- **Consecuencia:** cuando exista la pasarela (Fase 1), su puerto será el único
  que se publique.

### D06.11 — Memoria compartida y almacén de objetos acotados

- **Decisión:** `shm_size: 1gb` por contenedor y `--object-store-memory` de
  200 MB por nodo.
- **Porqué:** Docker da 64 MB de memoria compartida por defecto y Ray guarda ahí
  su almacén de objetos; con menos se degrada a disco y avisa. Acotar el almacén
  impide que tres nodos en una máquina de 8 GB se queden con la memoria. El
  sistema no usa el almacén de objetos de Ray para los artefactos (D02.7), así
  que 200 MB sobran.

### D06.12 — Integración continua en dos trabajos

- **Decisión:** `.github/workflows/ci.yml`, en cada push:
  1. **unit:** Python 3.12, pruebas unitarias con las advertencias tratadas
     como errores. Un minuto, sin Docker.
  2. **cluster**, solo si el anterior pasa: construye la imagen, levanta el
     clúster, ejecuta `tests/e2e` dentro del nodo de control, guarda los logs
     del clúster si falla y lo apaga siempre.
- **Porqué:** el primer trabajo da el aviso rápido y no gasta en construir la
  imagen si la lógica ya está rota (riesgo de 04 §4). Las pruebas de extremo a
  extremo corren dentro del contenedor porque necesitan Ray en Linux.
- **Acciones usadas:** solo `actions/checkout` y `actions/setup-python`, las
  oficiales de GitHub (aprobado en 04 §5). Permisos de solo lectura.
- **Pendiente:** la publicación de la imagen (entrega continua, T3) es de la
  Fase 8.

### D06.13 — Diseño del experimento

- **Decisión:** dos guiones en `experiments/ray_failures/`. `driver.py` corre en
  el nodo de control y es quien espera el resultado de la tarea. `run.py` corre
  en el anfitrión, rompe el trabajador con órdenes de Docker y anota lo que vio
  el driver, una línea JSON por observación, en `results.jsonl`.
- **Tres cuidados:**
  - La tarea se lanza con `max_retries=0`, como lo hará el sistema (D02.2): se
    mide lo que Ray informa, no lo que oculta reintentando.
  - El fallo se provoca solo cuando la tarea avisa de que ya está corriendo y
    en qué nodo (un actor auxiliar), para no romper un trabajador que todavía
    no la tiene.
  - Los tiempos se toman todos con el reloj del clúster. El anfitrión Windows y
    los contenedores no comparten reloj, y una resta entre relojes distintos no
    mide nada.
- **Qué no mide:** una máquina física que cae. Tres contenedores en un portátil
  comparten núcleo y hardware (Lab3); el experimento dice qué ve Ray cuando cae
  un *contenedor*.

## 3. Experimento de fallos de Ray

Hecho el 2026-11-15 con Ray 2.60.0 sobre el clúster de D06.7–D06.11, siguiendo
D06.13. Una ejecución por situación; quien espera el resultado aguarda como
máximo 180 s.
Datos en bruto: `experiments/ray_failures/results.jsonl`. Para repetirlo, con
el clúster levantado:

```
py experiments/ray_failures/run.py > experiments/ray_failures/results.jsonl
```

| Situación provocada | Se esperaba (D02.9) | Qué recibió quien espera | Cuándo | Al reparar el trabajador |
|---------------------|---------------------|--------------------------|--------|--------------------------|
| El comando termina con error | Resultado normal con código ≠ 0 | El valor devuelto | Al terminar | — |
| Muere el proceso que ejecuta | Aviso de proceso muerto | `WorkerCrashedError` | Al instante | El nodo sigue vivo y sirve otras tareas |
| Se mata el contenedor trabajador | Aviso de nodo muerto | `NodeDiedError` | 0,2 s | El contenedor queda parado |
| Se pausa el contenedor | Silencio; después, nodo declarado muerto | **Nada**: vence el plazo propio de 180 s | — | Al reanudarse, el nodo descubre que lo dieron por muerto y se cierra; quien espera recibe `NodeDiedError` |
| Se desconecta de la red | Igual que el anterior | **Nada**: vence el plazo propio de 180 s | — | Igual: al reconectarse se cierra y llega `NodeDiedError` |
| Se reconecta tras ser declarado muerto | El nodo no retoma su trabajo | Confirmado: el nodo termina su propio proceso y la tarea no continúa | — | Hay que reiniciar el contenedor para que vuelva como nodo nuevo |

Las dos excepciones de Ray heredan solo de `RayError`.

### Lo que confirma

- `TASK` es la clase fiable: el resultado llega como valor, sin excepción, y
  alguien vio terminar el comando.
- `WorkerCrashedError` y `NodeDiedError` son las dos señales de `WORKER`. No
  basta con capturar `RayError`, que también cubre el plazo vencido
  (`GetTimeoutError` hereda de él): se distinguen por la clase exacta.
- Un nodo dado por muerto no vuelve como zombi: al recuperar la red se cierra
  solo y sus tareas mueren con él.

### Lo que corrige

**Ray no avisa de un nodo pausado o incomunicado, al menos en 180 s.** Se
esperaba que lo declarara muerto en unas decenas de segundos y que eso llegara
como `NodeDiedError`. No llegó nada mientras duró el fallo; la excepción solo
apareció al repararlo. No se midió cuánto habría tardado con un fallo más
largo.

Consecuencias, recogidas en D06.14:

1. El plazo propio del coordinador por intento (F6.5) deja de ser un respaldo:
   es la **única** señal de un nodo incomunicado.
2. Un nodo pausado (proceso congelado, el nodo está «vivo») y uno incomunicado
   (el nodo está bien, la red no) son indistinguibles para el coordinador, como
   anticipaba D02.9: los dos se ven como silencio y se clasifican `NETWORK`.
3. Durante la desconexión el trabajador siguió ejecutando la tarea (el
   contenedor estaba en marcha). Es exactamente la situación en la que el
   coordinador relanza mientras el intento anterior sigue vivo, y podría
   terminar y guardar su resultado justo al recuperar la red, antes de cerrarse.
   El árbitro tiene que ser el guardado «el primero que escribe gana» del
   almacén (D02.10), no Ray.
4. Cuando la red vuelve, llega un `NodeDiedError` tardío para un intento que el
   coordinador ya dio por perdido. Debe ignorarse, igual que una confirmación
   tardía (F7.5).

### Por qué matar el contenedor se detecta en 0,2 s

`docker kill` destruye la red del contenedor y el sistema operativo cierra sus
conexiones: el otro extremo recibe el cierre al instante. Una máquina que se
queda sin corriente no cierra nada, y se vería como la fila de la pausa:
silencio. La detección rápida de esa fila es una propiedad de los contenedores
en una misma máquina (Lab3), no algo que el diseño pueda dar por hecho.

## 4. Decisiones derivadas del experimento

### D06.14 — Señales de fallo confirmadas y plazo propio obligatorio

- **Decisión:** se mantiene la taxonomía de D02.9 y se precisa qué señal produce
  cada clase en el coordinador:

  | Señal | Clase |
  |-------|-------|
  | `TaskResult` que no es éxito | `TASK` |
  | `WorkerCrashedError`, `NodeDiedError` | `WORKER` |
  | `ExecutorFailure` (D06.3) | `NETWORK` o `WORKER` según la evidencia |
  | Vence el plazo propio del intento | `NETWORK` |
  | Excepción de Ray para un intento ya abandonado | Ninguna: se ignora |

- **Porqué:** es lo que el experimento mostró que llega, con la clase exacta de
  cada excepción.
- **Consecuencia para la Fase 5:** el plazo por intento no puede faltar nunca.
  Propuesta para decidir allí: tiempo límite declarado de la tarea más un
  margen, porque el espacio aislado ya mata el comando al vencer su tiempo
  límite (F3.4); pasado ese margen sin noticias, solo queda un fallo de
  infraestructura.
