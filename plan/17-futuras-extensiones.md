# 17 — Registro de extensiones futuras (post-C7)

- **Fecha:** 2026-11-25
- **Estado:** vigente
- **Paso:** registro de extensiones candidatas tras el corte C7
- **Autor:** Juan Carlos (persona A, pista de control)

---

Este documento centraliza las extensiones que **no forman parte del alcance base** (R1–R9, cortes C0–C7) pero que:
- fueron analizadas durante el desarrollo,
- aportan valor demostrable al sistema,
- tienen diseño completo y riesgos identificados,
- pueden implementarse **después de la entrega** sin rediseñar la arquitectura.

Cada extensión recibe un identificador **Ex** y sigue la plantilla de la §2.

---

## 1. Índice de extensiones registradas

| ID  | Título                          | Requisito base | Estado       | Prioridad | Documento de diseño |
|-----|---------------------------------|----------------|--------------|-----------|---------------------|
| E1  | Deduplicación en vuelo (F9.6)   | R9 (extensión) | Propuesta    | Alta      | §3.1                |
| E2  | —                               | —              | —            | —         | —                   |
| E3  | —                               | —              | —            | —         | —                   |

> **Convención:** al añadir una nueva extensión, incrementar el número (E2, E3…), rellenar la fila en esta tabla y crear su sección en §3 usando la plantilla de §2.

---

## 2. Plantilla para nuevas extensiones (uso obligatorio)

Toda extensión nueva **debe** documentarse con las siguientes secciones, numeradas `D17.x` para sus decisiones:

### 2.1 Qué es y para qué sirve
- Definición en una frase.
- Escenario canónico (paso a paso).
- Diferencia con la funcionalidad base existente (tabla comparativa).

### 2.2 Análisis formal de pros y contras
| # | Beneficio / Coste | Justificación técnica |
|---|-------------------|----------------------|
| P1 | … | … |
| C1 | … | … |

### 2.3 Por qué NO está en el corte C7
- Lista de razones (alcance, dependencias, riesgos, prioridades).

### 2.4 Por qué DEBERÍA implementarse después
- Valor incremental.
- Infraestructura ya lista que lo habilita.
- Coste estimado.

### 2.5 Guía de implementación
#### 2.5.1 Decisiones de diseño (tabla `D17.x`)
| ID  | Decisión | Opción elegida | Razón |
|-----|----------|----------------|-------|
| D17.x | … | … | … |

#### 2.5.2 Cambios en componentes (tabla)
| Componente | Cambios |
|------------|---------|

#### 2.5.3 Algoritmo (pseudocódigo crítico)

#### 2.5.4 Manejo de casos frontera (reintentos, reinicios, caché, cancelación)

#### 2.5.5 Tests requeridos (mínimos para corte)
| Test | Descripción |

#### 2.5.6 Métricas de aceptación

### 2.6 Planificación sugerida
| Sprint | Entregable | Criterio de_done |

### 2.7 Riesgos residuales y mitigaciones
| Riesgo | Probabilidad | Impacto | Mitigación |

### 2.8 Conclusión y recomendación
- Veredicto: implementar / posponer / descartar.
- Condiciones de entrada (qué debe estar cerrado antes).

### 2.9 Trazabilidad
| Requisito | Feature | Estado | Evidencia |

---

## 3. Extensiones registradas

### 3.1 E1 — Deduplicación en vuelo (F9.6)

> **Origen:** feature F9.6 de `plan/01-analisis-de-features.md`; analizada en Fase 5 y Fase 8.  
> **Estado:** **Propuesta** — diseño completo, no implementada en C7.

#### 3.1.1 Qué es y para qué sirve

**Definición.** La **deduplicación en vuelo** (in-flight deduplication) es la capacidad del sistema para detectar que **dos o más tareas idénticas** (mismo comando, mismas entradas, mismos requisitos de recursos, mismo entorno) han sido enviadas al clúster **antes de que cualquiera de ellas haya terminado**, y hacer que **compartan una única ejecución** en lugar de lanzar instancias redundantes.

**Escenario canónico.**
1. El cliente envía el trabajo **W** que contiene las tareas **T₁** y **T₂**.
2. **T₁** y **T₂** son idénticas en su especificación completa (hash de especificación coincidente).
3. El coordinador recibe **T₁**, la pone en cola y lanza su ejecución en el trabajador **A**.
4. Antes de que **T₁** termine, el coordinador recibe **T₂**.
5. El coordinador detecta que **T₂** es duplicada de **T₁** (misma clave de deduplicación).
6. En lugar de encolar **T₂** en otro trabajador, el coordinador **vincula T₂ a la ejecución en curso de T₁**.
7. Cuando **T₁** termina (éxito o fallo), **T₂** recibe el mismo resultado **sin haber consumido recursos adicionales**.

**Diferencia con la caché normal (F9.1–F9.5).**

| Característica | Caché por contenido (F9.1–F9.5) | Deduplicación en vuelo (F9.6) |
|----------------|----------------------------------|-------------------------------|
| **Cuándo actúa** | Después de que una tarea termina y su resultado se guarda en el almacén. | Mientras la tarea original **aún se está ejecutando**. |
| **Clave** | Hash de entradas + especificación (clave de contenido). | Hash de especificación completa (incluye `resources`, `timeout`, etc.). |
| **Beneficio** | Evita re-ejecutar trabajo idéntico en trabajos **posteriores**. | Evita lanzar trabajo idéntico **concurrente** dentro del mismo trabajo o entre trabajos simultáneos. |
| **Garantía** | Resultado bit-a-bit idéntico al original. | Resultado bit-a-bit idéntico al original (misma ejecución). |
| **Coste de almacenamiento** | Requiere persistir artefactos y metadatos en `BlobStore` / `store`. | Solo requiere mantener una entrada en memoria en el coordinador mientras la tarea vuele. |

#### 3.1.2 Análisis formal de pros y contras

**Pros (beneficios).**

| # | Beneficio | Justificación |
|---|-----------|---------------|
| P1 | **Ahorro de recursos CPU/RAM/red** | Evita ejecutar la misma compilación o prueba pesada múltiples veces en paralelo. En CI/CD real, un mismo *workflow* puede disparar jobs idénticos en matrices; la deduplicación en vuelo elimina esa redundancia. |
| P2 | **Reducción de latencia cola-a-cola** | Las tareas duplicadas no esperan en cola ni compiten por trabajadores; reciben el resultado tan pronto como la original termina. |
| P3 | **Menor presión sobre el BlobStore** | Menos artefactos escritos y leídos (la tarea duplicada no genera su propia entrada en el almacén). |
| P4 | **Determinismo reforzado** | Garantiza que tareas idénticas en el mismo DAG producen **exactamente** la misma salida (misma ejecución, mismo entorno, misma semilla si la hay). |
| P5 | **Compatibilidad natural con reintentos** | Si la tarea original falla por `WORKER`/`NETWORK` y se reintenta, las tareas vinculadas siguen la misma suerte; no hay divergencia. |
| P6 | **Simplicidad conceptual para el usuario** | El cliente no necesita coordinar entre jobs; el sistema «hace lo correcto» automáticamente. |

**Contras (costes y riesgos).**

| # | Coste / Riesgo | Justificación |
|---|----------------|---------------|
| C1 | **Complejidad en el coordinador** | Requiere mantener un mapa en memoria `clave_dedupe → ejecución_en_vuelo`, gestionar ciclos de vida, limpieza en fallos y reinicios del coordinador (Fase 8 parte A). |
| C2 | **Estado adicional que reconciliar** | Si el coordinador cae y se recupera (diario SQLite), debe reconstruir el mapa de deduplicación en vuelo o aceptar que se pierda (ver §3.1.7). |
| C3 | **Acoplamiento temporal entre tareas** | La tarea duplicada **depende** de la original para su resultado; si la original se cancela por política externa, la duplicada también muere (semántica de «compartir suerte»). |
| C4 | **Dificultad de depuración** | Los logs y trazas muestran una sola ejecución para múltiples tareas; el usuario puede no entender por qué su tarea «no aparece» en el trabajador. |
| C5 | **Falsa deduplicación por colisión de hash** | Probabilidad despreciable con SHA-256, pero el diseño debe documentar que la clave es `sha256(spec_json)` y que una colisión implicaría compartir ejecución incorrecta. |
| C6 | **Interacción con caché persistente** | Si una tarea en vuelo tiene un duplicado en caché (ejecución previa), ¿se usa la caché o se espera la en vuelo? Requiere política clara (ver §3.1.5, D17.4). |
| C7 | **Impacto en aislamiento (R3)** | Compartir ejecución significa compartir contenedor/entorno; si la tarea original contamina el entorno (archivos temporales, estado global), la duplicada lo hereda. El sandbox actual (Fase 1) ya aísla por tarea, pero la deduplicación en vuelo **rompe** ese aislamiento intencionadamente. |

#### 3.1.3 Por qué NO se implementó en el sistema base (corte C7)

1. **Alcance mínimo viable (MVP)**: Los requisitos R1–R9 no exigen deduplicación en vuelo; la caché por contenido (F9.1–F9.5) cumple R9.
2. **Complejidad de estado distribuido**: El coordinador es *stateful* pero su estado persistente (diario SQLite) no fue diseñado para reconstruir mapas efímeros de ejecuciones en vuelo.
3. **Riesgo de acoplamiento oculto**: Compartir ejecución entre tareas rompe el modelo mental de «cada tarea se ejecuta en su sandbox aislado» (R3), lo que dificulta el razonamiento sobre fallos.
4. **Prioridad de la Fase 8 parte A**: La recuperación del coordinador (diario + replay) era prerrequisito para cualquier estado en memoria que valga la pena reconstruir; sin ella, la deduplicación en vuelo se pierde en cada reinicio.
5. **Pruebas de inyección de fallos insuficientes**: No hay harness para probar «dos tareas idénticas llegan con 50 ms de diferencia, la primera falla por WORKER, la segunda debe reintentar con la primera».
6. **Decisión explícita D09.3** (`plan/09-fase-5-fallos-y-reintentos.md`): «La deduplicación en vuelo queda fuera del alcance de la Fase 5 y se reevaluará en Fase 8 si el tiempo lo permite».

#### 3.1.4 Por qué DEBERÍA implementarse en Fase 8 (extensión)

1. **Valor demostrable en CI/CD real**: GitHub Actions y Buildkite no la tienen nativamente; sería una **ventaja competitiva** del sistema.
2. **Infraestructura ya lista**: Fase 8 parte A da recuperación del coordinador; el mapa en memoria puede reconstruirse repitiendo el diario (las tareas en vuelo aparecen como `DISPATCHED` sin `COMPLETED`).
3. **Bajo coste incremental**: ~200 líneas en `coordinator.py` + tests; no toca `executor`, `sandbox`, `BlobStore` ni `store`.
4. **Completitud de R9**: R9 dice «caché basada en contenido para evitar trabajo idéntico cuando las entradas no cambian»; la deduplicación en vuelo es la **extensión natural** al caso concurrente.

#### 3.1.5 Guía de implementación

**Decisiones de diseño (`D17.x`).**

| ID  | Decisión | Opción elegida | Razón |
|-----|----------|----------------|-------|
| D17.1 | **Clave de deduplicación** | `sha256(json_canonical(spec_task))` donde `spec_task` incluye: `command`, `inputs`, `outputs`, `resources`, `timeout`, `env`, `working_dir`. | Garantiza que tareas idénticas en todo lo que afecta el resultado compartan clave. Excluye `id` y `needs` (son metadata de grafo). |
| D17.2 | **Ámbito de deduplicación** | **Global al clúster** (todas las tareas de todos los jobs). | Máximo ahorro; la clave ya incluye toda la especificación. |
| D17.3 | **Vida de la entrada en mapa** | Desde `DISPATCHED` hasta `COMPLETED` / `FAILED` / `CANCELLED` de la tarea original. | Limpieza determinista; no hay fugas. |
| D17.4 | **Política caché vs. en vuelo** | **En vuelo gana**: si hay tarea en vuelo con la misma clave, se vincula a ella **aunque exista entrada en caché**. | Evita divergencia: la en vuelo puede tener fallos transitorios que la caché no refleja; además, la en vuelo es más fresca (mismo código, mismo entorno actual). |
| D17.5 | **Semántica de fallo compartido** | Si la tarea original falla con `TASK` (código de salida ≠ 0), la duplicada **también falla con TASK** (mismo código, mismos logs/artefactos). Si falla con `WORKER`/`NETWORK`/`TIMEOUT`, la duplicada **sigue a la original en su reintento** (misma suerte). | Consistencia fuerte; evita que una duplicada «tenga suerte» y pase mientras la original falla. |
| D17.6 | **Límite de tareas vinculadas** | Sin límite duro; métrica `inflight_dedupe_linked_total` para alertar si se disparan (ej. >1000 en un job). | Casos patológicos (matrices gigantes) se detectan por observabilidad, no por límite arbitrario. |
| D17.7 | **Persistencia del mapa** | **No persistir** en el diario SQLite. En reinicio del coordinador, el mapa se reconstruye escaneando tareas en estado `DISPATCHED` / `RUNNING` sin `COMPLETED`. | El diario ya tiene esa info; persistir el mapa duplicaría estado y complicaría la reconciliación. |
| D17.8 | **Observabilidad** | Eventos de log estructurados: `dedupe_hit` (clave, task_id_original, task_id_duplicada), `dedupe_miss`, `dedupe_release`. Métricas Prometheus: `inflight_dedupe_hits_total`, `inflight_dedupe_misses_total`, `inflight_dedupe_linked_gauge`. | Trazabilidad completa para auditoría y depuración. |

**Cambios en componentes.**

| Componente | Cambios |
|------------|---------|
| `dpes/coordinator.py` | Nuevo atributo `_inflight_dedupe: dict[str, InFlightEntry]` donde `InFlightEntry = (task_id_original, set[task_id_vinculadas], asyncio.Event terminado)`. Lógica en `_dispatch_ready()` antes de encolar en Ray. Nuevo método `_on_task_terminal(task_id, result)` que notifica a vinculadas y limpia entrada. |
| `dpes/messages.py` | Ninguno (la clave se computa desde `TaskSpec` existente). |
| `dpes/scheduler.py` | Ninguno (el grafo no cambia; la deduplicación es decisión de despacho). |
| `dpes/log.py` | Añadir `event_dedupe_hit`, `event_dedupe_miss`, `event_dedupe_release` (nivel `INFO`). |
| `dpes/metrics.py` (nuevo o extender) | Contadores y gauge de Prometheus para D17.8. |
| `tests/integration/test_inflight_dedupe.py` | Suite completa (ver §3.1.5). |

**Algoritmo (pseudocódigo).**

```python
# En Coordinator._dispatch_ready(task_id: str) -> None
spec = self._tasks[task_id].spec
key = hashlib.sha256(json_canonical(spec)).hexdigest()

if key in self._inflight_dedupe:
    original_id, linked_set, done_evt = self._inflight_dedupe[key]
    linked_set.add(task_id)
    self._log.event_dedupe_hit(key, original_id, task_id)
    metrics.inflight_dedupe_hits_total.inc()
    # No lanzar en Ray; esperar a que la original termine
    async def wait_and_propagate():
        await done_evt.wait()
        result = self._task_results[original_id]  # ya guardado por _on_task_terminal
        self._complete_task(task_id, result)      # mismo resultado, misma vista
    asyncio.create_task(wait_and_propagate())
    return

# No hay duplicado en vuelo: lanzar normalmente
self._inflight_dedupe[key] = (task_id, {task_id}, asyncio.Event())
self._log.event_dedupe_miss(key, task_id)
metrics.inflight_dedupe_misses_total.inc()
ray_task = executor.execute.remote(spec, ...)  # lanza en Ray
self._ray_futures[task_id] = ray_task
```

```python
# En Coordinator._on_task_terminal(task_id: str, result: TaskResult) -> None
# Buscar clave de esta tarea
key = self._task_key[task_id]  # guardado en _dispatch_ready
if key in self._inflight_dedupe:
    original_id, linked_set, done_evt = self._inflight_dedupe.pop(key)
    done_evt.set()
    metrics.inflight_dedupe_linked_gauge.dec(len(linked_set) - 1)
    for dup_id in linked_set:
        if dup_id != original_id:
            self._log.event_dedupe_release(key, original_id, dup_id)
            self._complete_task(dup_id, result)  # propaga resultado idéntico
```

**Manejo de reintentos.**
- La tarea original puede reintentar (política R8). El mapa `_inflight_dedupe` **permanece vigente** durante todos los reintentos de la original.
- Las tareas vinculadas **esperan el resultado final** (tras agotar reintentos o éxito).
- Si la original agota reintentos y queda `FAILED(WORKER|NETWORK|TIMEOUT)`, las vinculadas reciben ese mismo fallo.
- Si la original es `CANCELLED` (por timeout global del job o señal externa), las vinculadas reciben `CANCELLED`.

**Tests requeridos (mínimos para corte C8).**

| Test | Descripción |
|------|-------------|
| `test_dedupe_basic` | Dos tareas idénticas en mismo job; segunda llega antes de que la primera termine → comparten ejecución, mismo resultado. |
| `test_dedupe_across_jobs` | Job A y Job B envían tarea idéntica concurrentemente → comparten ejecución. |
| `test_dedupe_retry_shared` | Original falla `WORKER`, reintenta y pasa → vinculada recibe éxito final. |
| `test_dedupe_task_failure` | Original falla `TASK` (exit code 1) → vinculada falla `TASK` con mismo código/logs. |
| `test_dedupe_cache_precedence` | Existe entrada en caché para la clave; llega tarea y hay otra en vuelo → se usa la en vuelo (D17.4). |
| `test_dedupe_coordinator_restart` | Coordinador reinicia (Fase 8A) con tareas en `DISPATCHED`/`RUNNING` → mapa se reconstruye y deduplicación sigue funcionando. |
| `test_dedupe_cancel_propagation` | Job cancelado → original `CANCELLED` → vinculadas `CANCELLED`. |
| `test_dedupe_metrics` | Contadores `hits`, `misses`, gauge `linked` reflejan la actividad. |

**Métricas de aceptación.**
- **Cobertura**: ≥ 90 % en `coordinator.py` líneas nuevas.
- **Latencia añadida**: < 1 ms en `_dispatch_ready` (hash + dict lookup).
- **Memoria**: < 1 KB por entrada en vuelo (clave + referencias).
- **Cero regresiones**: Todas las pruebas de C0–C7 siguen en verde.

#### 3.1.6 Planificación sugerida para Fase 8 (extensión)

| Sprint | Entregable | Criterio de_done |
|--------|------------|------------------|
| **Sprint 1** (2 días) | Implementación núcleo: mapa, clave, hit/miss, propagación de resultado. Tests unitarios de `coordinator.py` (mockeando Ray). | `py -m unittest tests.unit.test_coordinator_dedupe` en verde. |
| **Sprint 2** (2 días) | Integración con Ray: `test_dedupe_basic`, `test_dedupe_across_jobs` en clúster Docker. Métricas y logs. | `docker compose ... exec head python -m unittest tests.integration.test_inflight_dedupe` en verde. |
| **Sprint 3** (1 día) | Reintentos y fallos compartidos: `test_dedupe_retry_shared`, `test_dedupe_task_failure`. | Tests de integración correspondientes en verde. |
| **Sprint 4** (1 día) | Recuperación tras reinicio coordinador: `test_dedupe_coordinator_restart`. Documentación en `plan/17-futuras-extensiones.md` (este doc) y actualización de `PROGRESO.md`. | Corte C8 en verde en CI; `PROGRESO.md` actualizado. |

**Total estimado**: 6 días-hombre (1 persona).  
**Paralelizable**: Sprint 1–2 son secuenciales; Sprint 3–4 pueden solaparse con T5 (comparación arquitectónica) de la Fase 8 parte A.

#### 3.1.7 Riesgos residuales y mitigaciones

| Riesgo | Probabilidad | Impacto | Mitigación |
|--------|--------------|---------|------------|
| R1: Fuga de entradas en mapa (tarea original nunca termina) | Baja | Media (memoria) | Timeout global del job (ya existe) limpia tareas huérfanas; añadir `max_inflight_age` configurable (default 24 h) con limpieza periódica. |
| R2: Condición de carrera entre `_dispatch_ready` y `_on_task_terminal` | Media | Alta (duplicada se lanza en Ray y también se vincula) | Usar `asyncio.Lock` por clave o `dict.setdefault` atómico + evento; test de estrés con 100 tareas concurrentes idénticas. |
| R3: Colisión de hash SHA-256 | Despreciable | Catastrófica (ejecución compartida incorrecta) | Documentar que la clave es `sha256`; en entorno adversarial, usar `sha3_256` o añadir `spec_version` en la clave. |
| R4: Divergencia semántica con caché persistente | Media | Media | Política D17.4 clara y testeada (`test_dedupe_cache_precedence`); log `WARN` si hay entrada en caché y se usa en vuelo. |
| R5: Dificultad para depurar en producción | Alta | Baja | Logs estructurados `dedupe_hit`/`release` con `task_id` original y duplicada; herramienta CLI `dpes.cli dedupe-status <job_id>` para inspeccionar. |

#### 3.1.8 Conclusión y recomendación

La **deduplicación en vuelo (F9.6)** es una extensión **de alto valor y coste moderado** que encaja naturalmente en la arquitectura tras la Fase 8 parte A (recuperación del coordinador). No es requerida por los requisitos base (R1–R9), pero **eleva el sistema a nivel de CI/CD profesional** (GitHub Actions, Buildkite no la tienen).

**Recomendación**: Implementar en Fase 8 como **extensión opcional con corte C8**, condicionada a:
1. Que la Fase 8 parte A (recuperación coordinador) esté **cerrada y en verde en CI**.
2. Que queden **≥ 4 días-hombre** antes de la congelación (2026-11-29).
3. Que los tests de estrés (R2) pasen en el clúster Docker.

Si no se cumplen, documentar como **deuda técnica conocida** en `plan/17-futuras-extensiones.md` (este documento) y reevaluar en siguiente iteración del proyecto.

#### 3.1.9 Trazabilidad

| Requisito | Feature | Estado | Evidencia |
|-----------|---------|--------|-----------|
| R9 (extensión) | F9.6 Deduplicación en vuelo | **Propuesta** | Este documento (`plan/17-futuras-extensiones.md`) §3.1 |
| — | D17.1–D17.8 | Decisiones de diseño | §3.1.5 |
| — | Tests C8 | Planificados | §3.1.5 |
| — | Riesgos | Identificados | §3.1.7 |

> **Nota**: Este documento **no** implica compromiso de implementación. Sirve como especificación completa para que, si se decide abordar F9.6, el trabajo sea directo y sin ambigüedades.

---

## 4. Espacio reservado para futuras extensiones

> **Instrucción:** Para añadir una nueva extensión (E2, E3, …):
> 1. Añadir una fila en la tabla del **§1 Índice**.
> 2. Crear una sección **§3.x** siguiendo **exactamente** la plantilla del **§2**.
> 3. Numerar sus decisiones como `D17.y` continuando la secuencia.
> 4. Actualizar esta sección con una referencia a la nueva sección.

### 4.1 E2 — [Título de la extensión]

*[Pendiente: rellenar usando la plantilla del §2]*

### 4.2 E3 — [Título de la extensión]

*[Pendiente: rellenar usando la plantilla del §2]*

---

## 5. Convención de este documento

1. **No se borra nada**: una extensión descartada se marca «descartada» en el índice y se deja su sección con la razón.
2. **Decisiones numeradas `D17.x`**: siguen el formato *contexto → opciones → decisión → porqué → consecuencias* usado en todo `plan/`.
3. **Idioma**: español para la documentación; identificadores y pseudocódigo en inglés.
4. **Trazabilidad**: cada extensión enlaza su requisito base (si aplica), sus decisiones, tests y riesgos.