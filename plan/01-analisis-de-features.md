# 01 — Análisis de features y alcance

- **Fecha:** 2026-11-14
- **Estado:** aprobado el 2026-11-14
- **Paso:** análisis previo a la implementación

Qué hay que construir para cumplir R1–R9, con qué profundidad, y qué queda
fuera. El *cómo* está en [02-arquitectura.md](02-arquitectura.md), el orden en
[03-fases-y-dependencias.md](03-fases-y-dependencias.md) y el calendario en
[04-plan-de-implementacion.md](04-plan-de-implementacion.md).

## 1. Criterio de alcance

### D01.1 — Prioridad anclada a la orientación

- **Contexto:** 16 días de trabajo y dos personas. La orientación lista nueve
  funcionalidades; cada una admite desde una versión de juguete hasta un
  producto. Hace falta una regla para decidir la profundidad y qué recortar.
- **Opciones:** (a) lista plana de features; (b) prioridad por valor percibido;
  (c) prioridad anclada al texto de la orientación.
- **Decisión:** (c), con cuatro niveles:
  - **M (imprescindible):** lo que la orientación nombra. Sin ello el requisito
    no se cumple.
  - **S (debería):** no se nombra, pero sin ello el M correspondiente no resiste
    la pregunta «¿y qué pasa si falla X?».
  - **C (podría):** mejora que solo entra si sobra tiempo.
  - **W (fuera):** descartado explícitamente, con su porqué (D01.8).
- **Porqué:** (a) no dice qué recortar cuando falte tiempo; (b) invita a añadir
  alcance que nadie pidió. (c) hace que «cumplir al pie de la letra» sea
  comprobable feature por feature.
- **Consecuencia:** ningún M se recorta; el orden de sacrificio de S y C está en
  03 §5.

Identificadores: `F<requisito>.<n>`. `T<n>` son transversales.

## 2. Features por requisito

### R1 — Recepción de trabajos con dependencias

**Lectura.** Un *trabajo* es un conjunto de *tareas* con dependencias entre
ellas. «Recepción» es una frontera de confianza: lo que entra se valida entero
y se acepta o se rechaza entero. «Los clientes» en plural implica varios
trabajos conviviendo.

| ID | Feature | Pri. |
|----|---------|------|
| F1.1 | Especificación del trabajo: tareas con comando, dependencias (`needs`), entradas, salidas, entorno, recursos, tiempo límite, política de reintento y de caché | M |
| F1.2 | Validación en la frontera: estructura, nombres únicos, dependencias existentes, sin ciclos, rutas relativas sin `..`, recursos que algún nodo pueda satisfacer. Rechazo con error explicativo; nada se encola a medias | M |
| F1.3 | API de envío y consulta de estado | M |
| F1.4 | Cliente de línea de comandos (`submit`, `status`, `wait`) | M |
| F1.5 | Varios trabajos concurrentes sin interferencia | M |
| F1.6 | Envío idempotente: reenviar con la misma clave devuelve el mismo trabajo | S |

F1.6 existe porque el envío es una llamada remota como cualquier otra: si la
respuesta se pierde, el cliente no sabe si el trabajo se creó (Lab1, «timeout
ambiguo») y reintentar a ciegas lo duplicaría.

### R2 — Planificación de un DAG de tareas

**Lectura.** El planificador decide *cuándo* puede empezar cada tarea (orden) y
*dónde* (recursos). El grafo se fija al enviar el trabajo.

| ID | Feature | Pri. |
|----|---------|------|
| F2.1 | Construcción del grafo, orden topológico y detección de ciclos (el error nombra el ciclo) | M |
| F2.2 | Despacho por conjunto de listos: una tarea arranca cuando todas sus dependencias terminaron con éxito; las independientes corren en paralelo | M |
| F2.3 | Requisitos de recursos por tarea (CPU, memoria, capacidades con nombre) y ubicación en un worker que los cumple | M |
| F2.4 | Propagación del fallo a los dependientes transitivos; las ramas independientes continúan | M |
| F2.5 | Máquina de estados explícita de tarea y de trabajo | M |
| F2.6 | Límite de intentos en vuelo (cola acotada) para que un trabajo enorme no ahogue al coordinador | S |

### R3 — Ejecución aislada en trabajadores

**Lectura.** «Aislada» tiene tres sentidos y hay que decir cuál se cubre (Lab3:
«procesos, contenedores y máquinas virtuales aíslan cosas distintas»): una tarea
no ve los ficheros de otra, no hereda estado de ejecuciones anteriores, y no
puede dejar procesos vivos ni agotar al worker.

| ID | Feature | Pri. |
|----|---------|------|
| F3.1 | Espacio de trabajo nuevo por intento, destruido al terminar | M |
| F3.2 | Solo las entradas declaradas se colocan en el espacio; solo las salidas declaradas se recogen | M |
| F3.3 | Entorno limpio: solo las variables declaradas más un mínimo fijo | M |
| F3.4 | Grupo de procesos propio; al vencer el tiempo límite se mata el árbol entero | M |
| F3.5 | Cada worker es un contenedor distinto: una tarea no comparte sistema de ficheros con las de otros workers | M |
| F3.6 | Límites del sistema operativo por tarea (memoria, número de procesos, tamaño de fichero) | S |
| F3.7 | Aislamiento de sistema de ficheros y red por tarea (namespaces o contenedor por tarea) | C |

### R4 — Transferencia de entradas y artefactos

**Lectura.** Tres recorridos: cliente → sistema (entradas), tarea → tarea
(artefactos intermedios, posiblemente entre workers distintos) y sistema →
cliente (artefactos finales). En los tres, lo que llega debe ser lo que se envió.

| ID | Feature | Pri. |
|----|---------|------|
| F4.1 | Almacén direccionado por contenido: un blob se identifica por su SHA-256 | M |
| F4.2 | Subida de entradas desde el cliente (un directorio → manifiesto ruta→digest) | M |
| F4.3 | Colocación de las entradas en el worker con verificación del digest | M |
| F4.4 | Recogida de las salidas declaradas y registro del manifiesto de artefactos | M |
| F4.5 | Paso de artefactos de una tarea a sus dependientes | M |
| F4.6 | Descarga de artefactos por el cliente | M |
| F4.7 | Subida incremental: el cliente solo envía los blobs que faltan | S |

### R5 — Registro y transmisión de logs

**Lectura.** «Registro» = queda guardado; «transmisión» = sale del worker
mientras la tarea corre, no al final. La prueba de que se transmite es que el
log de un worker que murió a mitad sigue disponible.

| ID | Feature | Pri. |
|----|---------|------|
| F5.1 | Captura de la salida estándar y de error de la tarea | M |
| F5.2 | Envío incremental durante la ejecución, en fragmentos numerados | M |
| F5.3 | Consulta por desplazamiento y seguimiento en vivo (`logs -f`) | M |
| F5.4 | Lo transmitido sobrevive a la muerte del worker | M |
| F5.5 | Historial de eventos del trabajo: transiciones, intentos, clase de fallo y decisión de reintento, correlacionados por identificador de ejecución | M |
| F5.6 | En un acierto de caché se sirve el log de la ejecución original | S |

F5.5 es el «registro» del propio sistema: sin él no hay evidencia que mostrar de
R6–R8 (el curso pide historias y trazas, no capturas de pantalla).

### R6 — Clasificación de fallos de tarea, trabajador y red

**Lectura.** La orientación pide distinguir «fallo de la tarea» de «fallo de
infraestructura», y dentro de este, trabajador y red. El coordinador no puede
*observar* la causa (Lab1: caída, lentitud y corte de red se ven igual desde
fuera); solo puede clasificar por la evidencia que tiene. La clasificación debe
decir qué evidencia respalda cada clase y con qué certeza.

| ID | Feature | Pri. |
|----|---------|------|
| F6.1 | Taxonomía de tres clases (tarea, trabajador, red) con la evidencia de cada una | M |
| F6.2 | Clasificador: función pura de la evidencia a la clase | M |
| F6.3 | El estado final del trabajo distingue fallo de la tarea de fallo de infraestructura | M |
| F6.4 | Inyección reproducible de un fallo de cada clase | M |
| F6.5 | Plazos del coordinador por intento: el silencio prolongado es evidencia | M |

### R7 — «La tarea terminó, pero se perdió la confirmación»

**Lectura.** Entre el instante en que la tarea termina y el instante en que el
coordinador se entera hay una ventana (Lab1, «punto de confirmación»). Si algo
falla ahí, el coordinador ve lo mismo que si la tarea nunca hubiera corrido.
Hay tres sub-casos y los tres se tratan: el worker terminó y no logra avisar; el
coordinador se cansa de esperar y relanza; y la confirmación vieja llega tarde,
cuando ya hay otro intento.

| ID | Feature | Pri. |
|----|---------|------|
| F7.1 | Identidad: cada intento lleva un identificador de ejecución; cada tarea de un trabajo tiene una clave única de resultado | M |
| F7.2 | Punto de confirmación explícito: el resultado se guarda de forma durable *antes* de avisar al coordinador | M |
| F7.3 | Guardado idempotente: el primero que escribe gana; repetirlo devuelve el resultado ganador | M |
| F7.4 | Reconciliación: ante cualquier fallo de infraestructura, el coordinador consulta el resultado guardado antes de re-ejecutar | M |
| F7.5 | Confirmaciones tardías o duplicadas se descartan sin efecto | M |
| F7.6 | El worker reintenta el guardado con el mismo identificador | M |
| F7.7 | El coordinador recupera su estado tras reiniciarse y reconcilia los intentos que dejó en vuelo | S |

### R8 — Políticas de reintento según el tipo de fallo

**Lectura.** Reintentar sirve si el fallo fue temporal y perjudica si no (Lab4:
repite el efecto y añade carga). La política depende de la clase.

| ID | Feature | Pri. |
|----|---------|------|
| F8.1 | Política por clase: el fallo de tarea no se reintenta; el de trabajador se reintenta de inmediato; el de red, con espera exponencial y aleatoriedad | M |
| F8.2 | Presupuesto de intentos por clase, configurable por tarea (incluye poner a cero los de infraestructura para tareas no repetibles) | M |
| F8.3 | Agotado el presupuesto, fallo definitivo que conserva la clase | M |
| F8.4 | El reintento por fallo de trabajador evita el nodo sospechoso | C |

### R9 — Caché basada en contenido

**Lectura.** «Trabajo idéntico» = mismo comando sobre las mismas entradas, sin
importar el nombre de la tarea, el trabajo o quién lo envió. «Cuando las
entradas no cambian» exige que la clave dependa del *contenido* de las entradas,
no de rutas ni fechas.

| ID | Feature | Pri. |
|----|---------|------|
| F9.1 | Clave de acción: hash del comando, el entorno, las entradas (ruta→digest), las salidas declaradas y la plataforma | M |
| F9.2 | Consulta antes de despachar: con acierto la tarea no se ejecuta y sus artefactos quedan disponibles | M |
| F9.3 | Solo se guardan los éxitos | M |
| F9.4 | Claves encadenadas: la clave de una tarea usa los digests de salida de sus dependencias, de modo que un cambio que no altera un artefacto no invalida lo que depende de él | M |
| F9.5 | Exclusión por tarea (`cache: false`) para tareas que no son función de sus entradas | S |
| F9.6 | Dos tareas idénticas en vuelo: la segunda espera a la primera en vez de ejecutarse | C |

La caché solo es correcta si la tarea es función de sus entradas declaradas. Eso
lo hace razonable F3.2/F3.3: aislamiento y caché son la misma propiedad vista
desde dos lados.

### Transversales

| ID | Feature | Pri. |
|----|---------|------|
| T1 | Imagen Docker y clúster local con varios nodos | M |
| T2 | Integración continua: pruebas unitarias, de integración y de fallos | M |
| T3 | Entrega continua: publicación de la imagen | M |
| T4 | Pruebas unitarias con `unittest` | M |
| T5 | Comparación arquitectónica con GitHub Actions, Buildkite y HTCondor | M |
| T6 | Demostración reproducible desde un clon limpio | M |

**Resumen:** 44 M, 7 S y 3 C en R1–R9, más 6 transversales M.

## 3. Decisiones de feature

### D01.2 — La especificación del trabajo es JSON

- **Opciones:** YAML (lo que usan GitHub Actions y Buildkite), TOML, JSON.
- **Decisión:** JSON.
- **Porqué:** leer YAML exige una librería externa y solo Ray está permitido;
  `json` está en la biblioteca estándar y sirve igual para el cuerpo HTTP. TOML
  se puede leer con `tomllib` pero no escribir, y anida mal un mapa de tareas.
- **Consecuencia:** menos cómodo de escribir a mano; aceptable para un servicio
  cuyo cliente normal es una herramienta.

### D01.3 — El comando es una lista de argumentos

- **Opciones:** cadena interpretada por un shell; lista `argv`.
- **Decisión:** lista. Quien quiera shell lo pide: `["sh", "-c", "..."]`.
- **Porqué:** con una cadena, el significado depende del shell del worker y de
  las reglas de entrecomillado, y dos cadenas distintas pueden ser el mismo
  comando (rompe F9.1). La lista es canónica.

### D01.4 — Si una tarea falla, sus dependientes se omiten y lo demás sigue

- **Opciones:** (a) abortar el trabajo entero al primer fallo; (b) omitir solo
  los dependientes transitivos.
- **Decisión:** (b). Los omitidos terminan en `SKIPPED`.
- **Porqué:** en un trabajo de pruebas interesa conocer *todos* los fallos, no
  el primero. (a) es un caso particular que se podría añadir como opción; no se
  añade porque nadie lo pidió.

### D01.5 — El estado final distingue `FAILED` de `ERROR`

- **Decisión:** un trabajo termina `SUCCEEDED`, `FAILED` (alguna tarea falló por
  sí misma) o `ERROR` (ninguna falló por sí misma, pero alguna agotó sus
  reintentos de infraestructura). Cada tarea conserva además su clase.
- **Porqué:** es la distinción que la orientación pide en su descripción
  («diferencias entre fallo de la tarea y fallo de infraestructura»). Para quien
  envía el trabajo son respuestas distintas: «tu código está roto» frente a «no
  pudimos comprobarlo; reenvía».
- **Alternativa descartada:** un único `FAILED` con un campo de motivo: obliga a
  cada cliente a mirar dentro para saber si debe reintentar.

### D01.6 — El fallo de tarea no se reintenta por defecto

- **Decisión:** presupuesto 0 para la clase tarea; configurable por tarea.
- **Porqué:** un comando que devolvió error lo devolverá otra vez con las
  mismas entradas; reintentarlo gasta recursos y retrasa la respuesta. El caso
  real que lo necesita (pruebas inestables) lo declara quien las escribe.

### D01.7 — Carga de demostración: un proyecto pequeño en C

- **Decisión:** el ejemplo de referencia compila varios `.c` por separado, los
  enlaza y ejecuta las pruebas: `compile:*` → `link` → `test`.
- **Porqué:** ejercita de forma natural todo R1–R9: dependencias reales,
  artefactos que viajan entre workers, y una caché cuyo efecto se ve (cambiar un
  `.c` recompila solo ese fichero).
- **Consecuencia:** la imagen de los workers incluye `gcc` y `make`. Son
  paquetes del sistema, no librerías de Python; se señala aquí para que conste.
- **Alternativa descartada:** solo tareas de Python: no hay artefactos
  intermedios con significado y la caché no se aprecia.

### D01.8 — Fuera de alcance

| Descartado | Porqué |
|------------|--------|
| Cancelación de trabajos | No está en la orientación. Añade una transición más a cada estado y otra carrera (cancelar frente a terminar) que habría que defender. |
| Autenticación, autorización, cuotas | No está en la orientación. Consecuencia que sí se documenta: el servicio ejecuta comandos arbitrarios por diseño, así que solo debe escuchar en una red de confianza. |
| Prioridades, equidad entre trabajos, expulsión | No está en la orientación; el orden entre trabajos es de llegada. |
| Grafo dinámico (matrices, condicionales, tareas que crean tareas) | La orientación pide un DAG; fijarlo al enviar permite validarlo entero en la frontera. |
| Limpieza y caducidad del almacén y de la caché | No afecta a la corrección; el almacén crece durante la vida del despliegue. |
| Ficheros grandes por partes | Los blobs viajan enteros con un tamaño máximo documentado. |
| Alta disponibilidad del nodo de control | Ver 02 §1: se promete durabilidad, no disponibilidad. |
