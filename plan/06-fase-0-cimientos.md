# 06 — Fase 0: cimientos y experimento con Ray

- **Fecha:** 2026-11-15
- **Estado:** en curso
- **Paso:** Fase 0 de [03-fases-y-dependencias.md](03-fases-y-dependencias.md)
- **Corte:** C0

## 1. Alcance y estado

| Parte | Quién | Estado |
|-------|:-----:|--------|
| Estructura del paquete | A | Hecho |
| Contratos entre componentes | A | Propuestos; se congelan al cerrar la Fase 1 (D03.3) |
| Andamiaje de pruebas | A | Hecho |
| Imagen y clúster de contenedores | B | Pendiente |
| Integración continua | B | Pendiente |
| Experimento de fallos de Ray | A y B | Bloqueado: necesita el clúster |

C0 no está cerrado hasta que las seis filas estén hechas.

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

## 3. Experimento de fallos de Ray

Pendiente (D03.2). Se rellena cuando exista el clúster de contenedores.

| Situación provocada | Se espera (D02.9) | Qué recibió el coordinador | Tiempo hasta recibirlo | Al volver el nodo |
|---------------------|-------------------|----------------------------|------------------------|-------------------|
| El comando termina con error | Resultado normal con código ≠ 0 | | | |
| Se mata el proceso ejecutor | Aviso de proceso muerto | | | |
| Se mata el contenedor trabajador | Aviso de nodo muerto | | | |
| Se pausa el contenedor | Silencio; después, nodo declarado muerto | | | |
| Se desconecta de la red | Igual que el anterior | | | |
| Se reconecta tras ser declarado muerto | El nodo no retoma su trabajo anterior | | | |

Si alguna fila contradice lo esperado, se corrige D02.9 con una decisión nueva
antes de empezar la Fase 5.
