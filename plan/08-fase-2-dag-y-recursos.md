# 08 — Fase 2: DAG y recursos

- **Fecha:** 2026-11-15
- **Estado:** cerrada el 2026-11-15. El corte C2 pasó en la CI de GitHub
  (ejecución 43736551521, commit `468b3b7`). R1 y R2 quedan cumplidos.
- **Paso:** Fase 2 de [03-fases-y-dependencias.md](03-fases-y-dependencias.md)
- **Requisitos:** R1 (recepción de trabajos con dependencias) y R2
  (planificación de un DAG de tareas)
- **Corte:** C2 — un trabajo en rombo (A → B y C → D) ejecuta B y C a la vez en
  trabajadores distintos; un trabajo con ciclo se rechaza nombrando el ciclo; si
  B falla, D queda `SKIPPED` y C termina; un recurso que ningún nodo tiene se
  rechaza al enviar; dos trabajos enviados a la vez terminan los dos.

## 1. Alcance

| Feature | Qué | Estado |
|---------|-----|--------|
| F1.1 | Especificación completa: dependencias y recursos | Hecho |
| F1.2 | Validación completa: dependencias existentes, sin ciclos, recursos posibles | Hecho |
| F1.5 | Varios trabajos concurrentes | Hecho |
| F2.1 | Detección de ciclos que nombra el ciclo | Hecho |
| F2.2 | Despacho por tareas listas; las independientes en paralelo | Hecho |
| F2.3 | Requisitos de recursos y ubicación | Hecho |
| F2.4 | Propagación del fallo a los dependientes | Hecho |
| F2.5 | Máquina de estados completa | Hecho (los reintentos llegan en la Fase 5) |
| F2.6 | Límite de intentos en vuelo (S) | Aplazada (D08.6) |

## 2. Decisiones

### D08.1 — Formato: `needs` y `resources`

```json
{
  "tasks": {
    "compile": {"command": ["make", "main.o"], "resources": {"cpu": 2}},
    "test": {
      "command": ["make", "test"],
      "needs": ["compile"],
      "resources": {"cpu": 1, "memory_mb": 256, "gcc": 1}
    }
  }
}
```

| Campo | Regla | Si falta |
|-------|-------|----------|
| `needs` | Lista de nombres de tareas del mismo trabajo, sin repetir y sin la propia | `[]` |
| `resources.cpu` | Número mayor que 0 | 1 |
| `resources.memory_mb` | Entero mayor que 0 | Sin reserva de memoria |
| Cualquier otra clave de `resources` | Capacidad con nombre: número mayor que 0. Nombre de `[a-z][a-z0-9_-]` hasta 64 caracteres | — |

- **`needs` como en GitHub Actions:** la referencia (00 §1) usa el mismo nombre
  para lo mismo; quien la conoce lo lee sin explicación.
- **Las capacidades con nombre** son lo que un trabajador declara tener (un
  compilador, una GPU, ser un trabajador concreto: D06.9). Solo minúsculas, y
  no pueden llamarse `memory` ni `object_store_memory`: Ray usa esos nombres
  para lo suyo y los nombres en mayúsculas (`CPU`, `GPU`) también son suyos.

### D08.2 — Validación del grafo y error que nombra el ciclo

- **Decisión:** cada nombre de `needs` debe ser una tarea del trabajo; una
  tarea no puede depender de sí misma ni repetir una dependencia; el grafo no
  puede tener ciclos. Si lo tiene, el error dice el camino:
  `tasks: dependency cycle a -> b -> c -> a`.
- **Algoritmo:** búsqueda en profundidad con tres colores, en orden de nombre
  para que el ciclo informado sea siempre el mismo. Lineal en tareas más
  dependencias. Vive en `dpes/dag.py`, sin dependencias de otros módulos.
- **Porqué nombrar el ciclo:** «hay un ciclo» en un trabajo de cincuenta tareas
  obliga a buscarlo a mano; el camino dice qué arista quitar.
- **Se levanta D07.3:** un trabajo puede tener cualquier número de tareas,
  desde una.

### D08.3 — Recursos: traducción a Ray y rechazo al enviar

- **Traducción:** `cpu` → `num_cpus`, `memory_mb` → `memory` en bytes, cada
  capacidad con nombre → recurso propio de Ray con ese nombre. Ray elige un
  trabajador que los tenga libres; si ninguno los tiene libres en ese momento,
  el intento espera en la cola de Ray.
- **Rechazo:** al enviar, si **ningún nodo vivo** tiene en total lo que pide
  una tarea, el trabajo entero se rechaza con `400`, nombrando la tarea y lo
  que pide. Se compara con un solo nodo, no con la suma del clúster, porque una
  tarea corre en un nodo.
- **Porqué rechazar en lugar de esperar:** un trabajo que pide una GPU en un
  clúster sin GPU esperaría para siempre en la cola de Ray sin que nadie lo
  dijera. Es un error del trabajo, y la frontera es el sitio para decirlo.
- **Dónde:** en el adaptador que usa la pasarela (`coordinator._Client`), que
  ya es un proceso de Ray y puede preguntar qué nodos hay. La pasarela traduce
  ese rechazo a `400` igual que un error de validación.
- **Límite:** se compara con la capacidad total, no con la libre, y con los
  nodos vivos en el momento del envío. Si después muere el único nodo capaz, la
  tarea espera en la cola: es el caso del plazo por intento (D07.13, Fase 5).

### D08.4 — Despacho por tareas listas; `READY` no se adopta

- **Decisión:** al aceptar un trabajo se lanzan las tareas sin dependencias.
  Cada vez que una tarea termina con éxito se lanzan, en el mismo paso, las que
  tenían todas sus dependencias con éxito. Las tareas independientes se lanzan
  a la vez; las que no caben esperan en la cola de Ray.
- **`READY` (propuesto en D05.3) no se adopta:** sin un límite propio de
  intentos en vuelo (D08.6), una tarea lista se lanza en el mismo paso en que
  queda lista, así que `READY` nunca sería observable. Se añade si llega F2.6.
- **`RUNNING` significa «hay un intento en vuelo»:** incluye el tiempo en la
  cola de Ray esperando recursos. Distinguir «en cola» de «ejecutándose»
  requeriría que el ejecutor avisara al empezar; llega con los eventos (F5.5,
  Fase 4).

### D08.5 — Una tarea sin éxito omite a sus dependientes, no al trabajo

- **Decisión:** cuando una tarea termina en `FAILED` o `ERROR`, todas las
  tareas que dependen de ella, directa o transitivamente, y siguen pendientes
  pasan a `SKIPPED`, con el motivo (`dependency 'b' did not succeed`). Las
  ramas independientes siguen. Es D01.4.
- **Estado del trabajo:** `SKIPPED` cuenta como terminado. El trabajo sigue la
  regla de D07.4: `FAILED` si alguna falló, si no `ERROR` si alguna dio error,
  si no `SUCCEEDED`. Una tarea omitida siempre viene de otra que falló, así que
  nunca hay un trabajo `SUCCEEDED` con tareas omitidas.

### D08.6 — F2.6 (límite de intentos en vuelo) aplazada

- **Decisión:** no se implementa en esta fase.
- **Porqué:** el riesgo que cubre (que un trabajo enorme ahogue al
  coordinador) lo amortigua Ray, que encola los intentos que no caben. Lo que
  cuesta cada intento en vuelo es memoria del coordinador, no CPU. Es una S y
  la tercera en el orden de recorte (D03.6); se hace si sobra tiempo.

### D08.7 — `dag.py` no depende de `spec.py`

- **Contexto:** el diagrama de módulos (05) dibujaba `dag → spec`.
- **Decisión:** al revés: `spec.py` usa `dag.find_cycle` para validar. `dag.py`
  trabaja sobre un mapa nombre → dependencias y no conoce el formato.
- **Porqué:** detectar ciclos no necesita saber qué es una tarea, y así no hay
  dependencia circular entre los dos. El diagrama se regenera en esta fase.

### D08.8 — La evidencia del paralelismo en C2 sale de las propias tareas

- **Decisión:** en la prueba de C2, cada tarea del rombo imprime su trabajador
  y la hora de inicio y fin. La prueba comprueba con eso que B y C corrieron en
  trabajadores distintos y a la vez, y que D empezó después de ambas.
- **Porqué:** el historial de eventos (F5.5) es de la Fase 4. Los relojes de
  los contenedores son el mismo reloj del anfitrión, así que las horas son
  comparables (no lo serían entre máquinas distintas).
- **Cómo se fuerza que vayan a trabajadores distintos:** B y C piden 2 CPU cada
  una, que es lo que tiene un trabajador entero (D06.9). Dos no caben en el
  mismo: es la ubicación por recursos de F2.3 haciendo su trabajo.

### D08.9 — La máquina de estados avanza por dependientes, no recorriendo todo

- **Decisión:** al crear un trabajo se calcula, para cada tarea, quién depende
  de ella. Cuando una tarea termina solo se examinan sus dependientes: los que
  ya tienen todas sus dependencias con éxito se lanzan; si terminó sin éxito, se
  omiten sus dependientes transitivos con un recorrido iterativo. Los intentos en
  vuelo se buscan por identificador de ejecución en un diccionario.
- **Porqué:** recorrer todas las tareas en cada evento haría el trabajo
  cuadrático en el número de tareas. Así cada arista del grafo se mira una vez.
  El recorrido es iterativo por lo mismo que en `dag.py`: un trabajo de 1 MB
  puede ser una cadena de decenas de miles de tareas.

## 3. Pruebas

| Nivel | Fichero | Qué comprueba |
|-------|---------|---------------|
| Unitarias | `tests/unit/test_dag.py` | Ciclos y su camino, mismo ciclo siempre, cadena de 20 000 tareas sin agotar la pila (D08.2) |
| Unitarias | `tests/unit/test_spec.py` | `needs`, `resources`, nombres de capacidades, traducción a Ray y rechazo por capacidad (D08.1–D08.3) |
| Unitarias | `tests/unit/test_scheduler.py` | Solo arrancan las raíces, las independientes salen juntas, se espera a todas las dependencias, la omisión transitiva y que un trabajo con omitidas no termina en éxito (D08.4, D08.5) |
| Unitarias | `tests/unit/test_gateway.py` | Un rechazo por capacidad es un `400` |
| Integración | `tests/integration/test_coordinator.py` | El actor recorre un rombo, omite solo lo que depende del fallo y el adaptador rechaza un recurso inexistente |
| Extremo a extremo | `tests/e2e/test_dag.py` | **Corte C2** completo |

Estado el 2026-11-15: 85 unitarias (en Windows, 2 módulos saltados por
necesitar Linux), 9 de integración y 11 de extremo a extremo (C0, C1 y C2) en
verde en el clúster.

En la prueba del rombo, B y C corrieron en trabajadores distintos con sus
intervalos solapados, y D empezó después de que terminaran las dos (D08.8).
