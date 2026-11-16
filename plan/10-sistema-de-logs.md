# 10 — Sistema de logs descriptivo

- **Fecha:** 2026-11-15
- **Estado:** hecho el 2026-11-15. La traza de un trabajo pasó en la CI de
  GitHub (ejecución 43747750827, commit `dce851f`)
- **Paso:** transversal, pedido aparte de las fases («además del proyecto»)

## 1. Qué es y qué no es

Es el **registro operativo del sistema**: cada componente describe en una línea
lo que hace y por qué, para poder reconstruir después qué pasó con un trabajo,
en qué nodo y en qué orden.

No es la **salida de las tareas** (R5, Fase 4: lo que imprime el comando,
transmitido en vivo) ni el **historial de intentos** de la vista (D09.6), que
es un dato del trabajo que se sirve al cliente. Los tres se complementan: el
historial dice qué decidió el sistema; la salida, qué hizo el comando; estos
logs, cómo llegó el sistema a cada decisión.

## 2. Decisiones

### D10.1 — `logging` de la biblioteca estándar, un logger por módulo

- **Decisión:** cada módulo con efectos escribe en `logging.getLogger(__name__)`
  (`dpes.gateway`, `dpes.coordinator`, `dpes.executor`). Un módulo nuevo,
  `dpes/log.py`, configura la salida una vez por proceso y da el formato.
- **Porqué:** no añade dependencias (regla de `CLAUDE.md`), y cualquier
  biblioteca o herramienta que lea logs de Python lo entiende.

### D10.2 — Una línea por suceso: mensaje y campos `clave=valor`

```
2026-11-15T21:04:05.123Z INFO    coordinator attempt launched execution=4f1c…/compile/0/1 cpu=1.0
2026-11-15T21:04:05.310Z INFO    executor    command finished execution=4f1c…/compile/0/1 exit_code=0 timed_out=False duration_s=0.18
```

- Hora en UTC con milisegundos, nivel, componente, un mensaje fijo en inglés
  (D0.2) y los datos como `clave=valor`; un valor con espacios o comillas va
  entre comillas, escapado.
- **Opciones:** texto libre; JSON por línea; mensaje fijo más campos.
- **Porqué:** el texto libre no se puede filtrar con fiabilidad; JSON se filtra
  bien pero cuesta leerlo a ojo en `docker compose logs`. Un mensaje fijo con
  campos se lee de corrido y se filtra con `grep execution=<id>`.

### D10.3 — El identificador de ejecución atraviesa todos los procesos

- **Decisión:** toda línea sobre un intento lleva `execution=<id>`, que ya
  contiene el trabajo, la tarea, la ronda y el número de intento (D07.5); toda
  línea sobre un trabajo lleva `job=<id>`.
- **Porqué:** es lo que pide Lab4: el mismo identificador sigue a la operación
  por los logs de todos los procesos. Con un solo `grep` se ve la pasarela que
  aceptó el trabajo, el coordinador que lanzó el intento en el nodo de control
  y el ejecutor que lo corrió en un trabajador.

### D10.4 — Los módulos puros no escriben logs

- **Decisión:** `spec`, `dag`, `failures`, `scheduler` y `messages` no
  escriben nada. Escriben las capas que los rodean (pasarela, coordinador,
  ejecutor), con lo que esos módulos devuelven.
- **Porqué:** escribir un log es un efecto; los módulos puros siguen sin
  efectos ni dependencias de entorno (D02.3), y sus pruebas no cambian.

### D10.5 — Qué se registra y con qué nivel

| Componente | `INFO` (lo normal) | `WARNING` (algo falló y el sistema lo gestiona) | `ERROR` (el sistema no pudo) |
|------------|--------------------|-----------------------------------------------|------------------------------|
| Pasarela | arranque, trabajo aceptado | trabajo o petición rechazados | coordinador no disponible |
| Coordinador | arranque, trabajo iniciado, intento lanzado, intento correcto, trabajo terminado con éxito | intento fallido (clase y motivo), plazo vencido, reintento programado | trabajo terminado en `FAILED` o `ERROR` |
| Ejecutor | intento en marcha (con el trabajador), comando terminado | fallo inyectado | sin espacio de trabajo |

- Las consultas de estado, que un cliente repite cada fracción de segundo, van
  en `DEBUG`: en `INFO` taparían todo lo demás.
- **El nivel se cambia** con la variable `DPES_LOG_LEVEL` (por defecto `INFO`).

### D10.6 — Nunca se registra el entorno ni la salida del comando

- **Decisión:** de una tarea solo se registra el programa (`argv[0]`) y el
  número de argumentos; nunca el valor de sus variables de entorno ni lo que
  imprime.
- **Porqué:** los trabajos de compilación y pruebas llevan credenciales en
  variables o argumentos. Un log se copia, se adjunta a incidencias y se guarda
  más tiempo que el trabajo. La salida tiene su propio canal (Fase 4).

### D10.7 — Todo a la salida de error del proceso; Ray lo reúne en el nodo de control

- **Decisión:** cada proceso escribe en su salida de error. El coordinador y los
  ejecutores son procesos de Ray: Ray reenvía su salida al proceso que los creó,
  la pasarela, cuya salida es la del contenedor del nodo de control. Así
  `docker compose -f docker/compose.yaml logs head` muestra los tres
  componentes juntos y en orden.
- **Configuración por proceso:** la pasarela la hace al arrancar; el actor del
  coordinador, al construirse; cada intento, en el envoltorio que lo lanza
  (D09.4), antes de llamar al ejecutor. El ejecutor sigue sin conocer Ray.
- **Ray agrupa líneas parecidas** («repeated 3x across cluster») tratando como
  iguales las palabras con números, y los identificadores tienen números: se
  perderían líneas de intentos distintos. Se desactiva con `RAY_DEDUP_LOGS=0`
  en el nodo de control.
- **Límite:** el reenvío va al proceso que creó el actor. Si la pasarela se
  reinicia sin reiniciar el clúster, el actor sigue siendo del proceso
  anterior y sus líneas ya no aparecen en el contenedor; quedan en los ficheros
  de Ray de cada nodo (`/tmp/ray/session_latest/logs`).

### D10.8 — El logger de la pasarela tiene nombre fijo

- **Contexto:** la primera versión usaba `logging.getLogger(__name__)` en todos
  los módulos. La pasarela se arranca con `python -m dpes.gateway`, y un
  módulo ejecutado así se llama `__main__`: su logger quedaba fuera del árbol
  `dpes` que configura `setup()` y todas sus líneas se descartaban. Las pruebas
  unitarias no lo vieron porque importan el módulo, y entonces sí se llama
  `dpes.gateway`. Lo encontró la prueba manual sobre el clúster.
- **Decisión:** la pasarela usa `logging.getLogger("dpes.gateway")`. Los demás
  módulos se importan siempre y conservan `__name__`. La prueba desde el
  anfitrión (§3) comprueba la línea de la pasarela en el clúster real.

### D10.9 — Las pruebas que corren en el anfitrión van juntas en `tests/host`

- **Decisión:** la prueba de caída real de la Fase 5 (`tests/chaos`) y la de
  logs comparten carpeta: `tests/host`, «pruebas que corren fuera del clúster y
  usan Docker». Se renombró la carpeta y sus referencias en la CI, `CLAUDE.md`
  y `plan/09`.
- **Porqué:** las dos necesitan lo mismo (el clúster levantado, la CLI de
  Docker y el puerto publicado) y se ejecutan con el mismo comando.

## 2.1 Ampliación del 2026-11-16: lo construido después

Las Fases 3 y 6 llegaron después de este documento. La 6 ya escribía sus
trazas en el coordinador (trabajo reenviado, resultado adoptado por
reconciliación, reconciliación fallida) y en el ejecutor (resultado
confirmado). Faltaban las que siguen.

### D10.10 — Los módulos que tocan disco pueden escribir logs

- **Contexto:** D10.4 deja sin logs a los módulos *puros*. `store.py` y
  `cas.py` no son puros: escriben en disco.
- **Decisión:** `store.py` registra quién gana al confirmar un resultado,
  porque es la evidencia central de R7 y solo ese módulo sabe si su inserción
  fue la que quedó. `cas.py` no registra: las subidas ya las registran la
  pasarela (las del cliente) y el ejecutor (las de las salidas), y una línea
  por blob repetiría lo mismo.
- **Porqué en `store.py` y no en el actor que lo envuelve:** así la prueba
  unitaria lo comprueba en Windows, sin Ray.

### D10.11 — Las trazas nuevas

| Componente | Mensaje | Nivel | Cuándo |
|------------|---------|-------|--------|
| almacén de resultados | `result saved` | INFO | Este intento es el primero en confirmar su clave |
| almacén de resultados | `result save repeated` | INFO | El mismo intento vuelve a guardar (su respuesta anterior se perdió, D02.10 regla 3) |
| almacén de resultados | `result already confirmed` | WARNING | Otro intento confirmó antes; se registran los dos identificadores (D02.10 regla 5) |
| almacén | `store started` | INFO | Arranque, con las rutas de blobs y resultados |
| ejecutor | `inputs placed` | INFO | Entradas colocadas: número de ficheros y bytes |
| ejecutor | `outputs stored` | INFO | Salidas guardadas: cuántas y cuántas faltaron |
| ejecutor | `result save failed` | WARNING | Un guardado del resultado falló y se reintentará, con la espera |
| pasarela | `blob served` | INFO | Descarga de un artefacto |
| pasarela | `job accepted` | INFO | Gana el campo `idempotency_key` cuando el envío lo trae |

### D10.12 — La clave de idempotencia sí se registra

- **Decisión:** se registra tal cual.
- **Porqué:** la elige el cliente para correlacionar sus reenvíos (D12.7) y no
  es un secreto; su alfabeto está restringido, así que no puede romper la
  línea. Sin ella no se podría seguir un reenvío desde el log de la pasarela.

## 3. Pruebas

| Nivel | Fichero | Qué comprueba |
|-------|---------|---------------|
| Unitarias | `tests/unit/test_log.py` | El formato de D10.2, el entrecomillado y escapado de valores, que un valor no rompe la línea, la excepción tras su línea, `setup` idempotente, el nivel desde `DPES_LOG_LEVEL` y que el paquete no imprime nada sin `setup` |
| Unitarias | `tests/unit/test_gateway.py` | Qué registra la pasarela al aceptar, rechazar o no llegar al coordinador, y que las consultas de estado quedan por debajo de `INFO` (D10.5) |
| Unitarias (Linux) | `tests/unit/test_executor.py` | El ejecutor registra dónde corrió y cómo terminó con el identificador de ejecución, el fallo inyectado y la falta de espacio, y nunca el valor de una variable de entorno ni la salida (D10.6) |
| Unitarias | `tests/unit/test_store.py` | Las tres salidas de un guardado (D10.11): primero en confirmar, repetición del mismo intento y otro intento que llega tarde, con los dos identificadores |
| Anfitrión | `tests/host/test_logs.py` | Un trabajo de dos tareas en el clúster real: cada proceso describe su parte con el mismo identificador (D10.3), incluido el almacén al confirmar el resultado; las líneas siguen el orden causal, el ejecutor nombra su trabajador, el fallo lleva su clase, el trabajo aceptado lleva la clave de idempotencia de la CLI y un secreto de prueba no aparece en todo el log del nodo de control |

Desde el 2026-11-16 (§2.1), `test_gateway.py` comprueba además la descarga de
un artefacto y la clave de idempotencia, y `test_executor.py` las entradas
colocadas, las salidas guardadas y el aviso de un guardado fallido antes de su
reintento. Las líneas de entradas y salidas solo aparecen si el intento tiene
alguna: en uno sin ellas serían ruido.

Estado el 2026-11-16, todo en verde sobre el clúster local: 204 unitarias en
el contenedor como `root` y sin privilegios (164 en Windows), 17 de
integración, 22 de extremo a extremo y 8 desde el anfitrión. En el log del
nodo de control aparecen `store started`, `result saved`, `inputs placed`,
`outputs stored`, `blob served` y la clave en `job accepted`.
`result save repeated` y `result already confirmed` solo se ven en las
unitarias: en el clúster, tras perder la confirmación, el coordinador adopta
el resultado guardado (D12.4) y nadie vuelve a guardar.

Estado el 2026-11-15, todo en verde: 142 unitarias en el contenedor (119 en
Windows), 12 de integración, 16 de extremo a extremo y 7 desde el anfitrión.

La prueba del orden causal partía de una expectativa equivocada: «job
accepted» de la pasarela no es lo primero. La pasarela lo escribe cuando el
coordinador ya confirmó el trabajo, así que va después de «job started». Es
el orden correcto (no se anuncia como aceptado lo que no está confirmado) y la
prueba lo comprueba así.

## 4. Cómo leer los logs

Con el clúster levantado, todo el recorrido de un trabajo:

```
docker logs dpes-head 2>&1 | grep <id del trabajo>
```

Un solo intento: `grep <id>/<tarea>/<ronda>/<intento>`. Para más detalle,
arrancar el clúster con `DPES_LOG_LEVEL=DEBUG` en el entorno de los servicios.
