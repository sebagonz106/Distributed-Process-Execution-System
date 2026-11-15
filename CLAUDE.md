# CLAUDE.md

Proyecto final de **Sistemas Distribuidos** (Ciencia de la Computación, MATCOM,
4.º año): **P17 — Ejecutor distribuido de compilación y pruebas**.
Todo lo que se entregue debe estar a nivel universitario de CS: cada decisión se
justifica con el modelo de fallos y las garantías que se defienden.

Repo: <https://github.com/sebagonz106/Distributed-Process-Execution-System>
(rama por defecto `main`; se trabaja con permiso de escritura como colaborador).

## Orientación — se cumple al pie de la letra

**Descripción.** Los sistemas de integración continua convierten una revisión de
código y un flujo de trabajo en numerosas tareas ejecutables. El sistema debe
modelar dependencias, aislamiento, registros, artefactos, caché y diferencias
entre fallo de la tarea y fallo de infraestructura.

**Objetivo general.** Construir un servicio donde los clientes envíen trabajos de
compilación o pruebas con comandos, dependencias, entradas y requisitos de
recursos; los trabajadores ejecutan las tareas en entornos aislados y devuelven
estado, registros y artefactos.

**Funcionalidades y alcance** (IDs usados en `plan/` y en las pruebas):

- **R1** Recepción de trabajos con dependencias.
- **R2** Planificación de un grafo acíclico dirigido (DAG) de tareas.
- **R3** Ejecución aislada en trabajadores.
- **R4** Transferencia de entradas y artefactos.
- **R5** Registro y transmisión de logs.
- **R6** Clasificación de fallos de tarea, trabajador y red.
- **R7** Tratamiento del caso «la tarea terminó, pero se perdió la confirmación».
- **R8** Políticas de reintento según el tipo de fallo.
- **R9** Caché basada en contenido para evitar trabajo idéntico cuando las entradas no cambian.

**Referencias para comparación arquitectónica:** GitHub Actions (flujos, trabajos,
ejecutores), Buildkite (agentes que obtienen trabajo, transmiten logs y publican
artefactos), HTCondor (ejecución distribuida por lotes).

No se añade alcance fuera de R1–R9 sin que el usuario lo pida.

## Reglas obligatorias

1. **Commits sin coautor.** Nunca añadir `Co-Authored-By`, «Generated with» ni
   ninguna atribución a IA en commits o PRs. El único autor es la identidad de
   git local. Solo se hace commit o push cuando el usuario lo pide.
   **No se hacen PR ni ramas: todo, código y documentación, se sube con push
   directo a `main`** (D04.8, que sustituye a D04.3). Nunca abrir una solicitud
   de cambio ni proponerla. La revisión del cambio se hace antes de subirlo.
2. **Stack:** Python + Ray. Docker para ejecutar y desplegar.
3. **Librerías externas: preguntar siempre antes de usarlas.** Por defecto la
   única dependencia externa permitida es Ray; lo demás se resuelve con la
   biblioteca estándar de Python. Antes de añadir cualquier otra librería
   (también las de pruebas, desarrollo o CI) hay que pedir autorización al
   usuario, indicando para qué se necesita y qué costaría hacerlo sin ella.
4. **`plan/` es parte del entregable.** Cada paso documenta cada decisión
   estructural y su porqué *antes* de escribir el código (ver abajo).
5. **Código limpio y revisado.** Usar el skill `ponytail` (mínimo código que
   resuelve el requisito completo) y revisar cada cambio antes de darlo por hecho.
6. **Pruebas unitarias y CI/CD.** Ningún paso se cierra sin pruebas que lo
   evidencien y sin el pipeline en verde.
7. **Sin suposiciones silenciosas.** Si hay varias lecturas de la orientación, se
   presentan al usuario; no se elige por omisión.

## Documentación de decisiones (`plan/`)

- Un archivo por paso: `plan/NN-titulo.md`, en español.
- Decisiones numeradas `DNN.k` con: contexto → opciones consideradas → decisión →
  porqué → consecuencias. Las alternativas descartadas se dejan escritas.
- Una decisión que cambia no se borra: se marca «sustituida por …».
- Al cerrar un paso se actualiza la tabla de trazabilidad R1–R9 en
  [plan/00-contexto-y-restricciones.md](plan/00-contexto-y-restricciones.md),
  que también lista las decisiones abiertas (A1–A10) y los riesgos.

## Flujo de trabajo por paso

1. Acordar el alcance del paso con el usuario.
2. Escribir `plan/NN-*.md` con las decisiones.
3. Pruebas primero, luego la implementación mínima.
4. Revisión del diff y verificación (pruebas + CI) antes de afirmar que está hecho.
5. Actualizar trazabilidad y [PROGRESO.md](PROGRESO.md), el tablero de un
   vistazo: icono de la fase, barra y fecha.

## Convenciones

- Documentación en español; identificadores, comentarios y mensajes de commit en
  inglés (D0.2, confirmada).
- Comentarios solo para el porqué que el código no puede mostrar.

## Recursos de la asignatura

Carpeta (solo lectura, **no** copiar al repo):
`C:\Users\Juank\Carrera\4to Año\Primer Semestre\Distribuidos`

- Sitio del curso: <https://matcom.github.io/distributed-systems-2026> (12 capítulos).
- `Conferencias/Conf 1..4/*.zip`: PDF + audios. Conf 3 «El nodo y sus fronteras»,
  Conf 4 «Comunicación». `Conf 5` solo tiene audios `.ogg`.
- `Labs/Lab1..4/*.zip`: notas del profesor (`.md`/`.pdf`) + scripts.
  - **Lab1** — timeout ambiguo, visión parcial, punto de confirmación → base de R6/R7.
  - **Lab2** — estilos de arquitectura y middleware → comparación arquitectónica.
  - **Lab3** — concurrencia, aislamiento, colas acotadas, apagado → R3 y el worker.
  - **Lab4** — RPC, reintentos con ID, deduplicación, respuesta tardía, reinicio → R7/R8.

Para consultarlos, extraer el zip al directorio temporal de la sesión, nunca
dentro del repo. Citar el material del curso en `plan/` cuando respalde una decisión.

## Entorno

- Windows 11; shell PowerShell, Git Bash disponible.
- Python 3.13 mediante el lanzador `py` (`python` no está en el PATH).
- Docker 29 instalado. Ray tiene soporte limitado en Windows: el clúster y las
  pruebas de integración corren en contenedores Linux.
- `gh` autenticado con la cuenta del autor de los commits.

## Plan vigente

Leer antes de tocar código. Aprobado por el usuario el 2026-11-14; cambiar una
decisión aprobada requiere una decisión nueva que la sustituya.

- [plan/01-analisis-de-features.md](plan/01-analisis-de-features.md): features
  `F<req>.<n>` por requisito con prioridad M/S/C/W y lo que queda fuera.
- [plan/02-arquitectura.md](plan/02-arquitectura.md): modelo de fallos,
  garantías S1–S4 y L1, y decisiones `D02.k`. Idea central: Ray como sustrato
  (clúster, colocación, transporte) con cada intento como tarea de Ray **sin
  reintentos**; orden, clasificación, reintentos, confirmación y caché son
  código propio. El coordinador es un núcleo puro dentro de un actor.
- [plan/03-fases-y-dependencias.md](plan/03-fases-y-dependencias.md): Fases 0–8,
  cortes funcionales C0–C8, mapa de dependencias y línea de corte.
- [plan/04-plan-de-implementacion.md](plan/04-plan-de-implementacion.md):
  calendario para dos personas (pista de control y pista de datos), puntos de
  control el 22 y el 26 de noviembre, congelación el 29, entrega el 1 de
  diciembre de 2026.

- [plan/05-diagramas.md](plan/05-diagramas.md): siete diagramas (dependencias
  entre fases y entre módulos, flujo de un trabajo, decisión del coordinador,
  ejecutor, estados de una tarea, confirmación perdida). Cada uno es un par
  `plan/diagramas/<nombre>.mmd` + `.svg`; al cambiar la fuente hay que volver a
  generar la imagen (D05.1).

- [plan/06-fase-0-cimientos.md](plan/06-fase-0-cimientos.md): Fase 0. Los
  contratos entre componentes viven en `dpes/messages.py` (D06.1) y se congelan
  al cerrar la Fase 1. No se crean módulos vacíos (D06.6).

Al empezar una fase se crea su documento con el siguiente número libre (D04.5).

## Pruebas

Desde la raíz del repo (D06.5): `py -m unittest discover -s tests/unit`
(`python` en vez de `py` dentro de los contenedores).

Clúster (un nodo de control y dos trabajadores, D06.7–D06.11). Docker Desktop
tiene que estar en marcha:

- Levantar: `docker compose -f docker/compose.yaml up -d --build`
- Pruebas de integración y de extremo a extremo, dentro del nodo de control:
  `docker compose -f docker/compose.yaml exec -T head python -m unittest discover -s tests/integration`
  (y lo mismo con `tests/e2e`). Tras cambiar código hay que volver a construir
  la imagen (`up -d --build`): el código va copiado dentro, no montado.
- Apagar: `docker compose -f docker/compose.yaml down`

La CI (`.github/workflows/ci.yml`) corre en cada push las unitarias fuera del
contenedor y, sobre el clúster, las unitarias como `root`, las de integración y
las de extremo a extremo (D06.12). Las pruebas del espacio aislado y del
ejecutor necesitan Linux: en Windows se saltan.

## Estado

Fase 0 cerrada el 2026-11-15, con la CI en verde en GitHub (ver
[PROGRESO.md](PROGRESO.md)): paquete `dpes`, contratos, imagen, clúster de tres
nodos, CI y experimento de fallos de Ray. La persona A hizo también la parte de
B en esta fase.

Fase 1 hecha en local ([plan/07](plan/07-fase-1-una-tarea.md)): `spec.py`,
`scheduler.py`, `gateway.py`, `cli.py`, `coordinator.py`, `sandbox.py` y
`executor.py`; la pasarela arranca con el nodo de control en
`127.0.0.1:8000`. La persona A hizo también la parte de B. Se cierra cuando la
CI pase en GitHub. Límite conocido hasta la Fase 5: sin plazo por intento
(D07.13).

Uso desde Windows con el clúster levantado:
`py -m dpes.cli submit job.json`, `py -m dpes.cli wait <id>`.

Hallazgo que condiciona la Fase 5 (D06.14): Ray no avisa de un trabajador
pausado o incomunicado; el plazo propio del coordinador por intento es la única
señal de ese fallo.

El usuario de estas sesiones (Juan Carlos) es la **persona A, pista de
control** (D04.7): especificación, grafo, coordinador, clasificación de fallos,
reintentos y confirmación. Trabaja en Windows: lo puro se prueba con
`py -m unittest`; lo que necesita Ray se ejecuta en los contenedores. La pista
de datos y plataforma (imagen, CI, espacio aislado, ejecutor, almacén, logs,
caché) es del otro integrante; no tocar sus módulos sin acordarlo.
