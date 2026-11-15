# 00 — Contexto, alcance y restricciones

- **Fecha:** 2026-11-14
- **Estado:** vigente
- **Paso:** preparación del proyecto (todavía no hay código)

Este documento fija el punto de partida: qué se pide, qué viene impuesto y qué
queda por decidir. Cada paso posterior añade un documento numerado en esta
carpeta (ver «Convención» al final).

## 1. Orientación (textual)

**P17 · Proyecto final — Ejecutor distribuido de compilación y pruebas**

**Descripción.** Los sistemas de integración continua convierten una revisión de
código y un flujo de trabajo en numerosas tareas ejecutables. El sistema debe
modelar dependencias, aislamiento, registros, artefactos, caché y diferencias
entre fallo de la tarea y fallo de infraestructura.

**Objetivo general.** Construir un servicio donde los clientes envíen trabajos de
compilación o pruebas con comandos, dependencias, entradas y requisitos de
recursos; los trabajadores ejecutan las tareas en entornos aislados y devuelven
estado, registros y artefactos.

**Referencias para comparación arquitectónica.**

- GitHub Actions: flujos, trabajos y ejecutores.
- Buildkite: agentes que obtienen trabajo, transmiten logs y publican artefactos.
- HTCondor: ejecución distribuida por lotes.

## 2. Trazabilidad de requisitos

La orientación se cumple al pie de la letra: cada funcionalidad tiene un
identificador y debe quedar enlazada al paso que la resuelve y a sus pruebas.

| ID | Funcionalidad (texto de la orientación) | Paso | Estado |
|----|------------------------------------------|------|--------|
| R1 | Recepción de trabajos con dependencias | — | pendiente |
| R2 | Planificación de un grafo acíclico dirigido (DAG) de tareas | — | pendiente |
| R3 | Ejecución aislada en trabajadores | — | pendiente |
| R4 | Transferencia de entradas y artefactos | — | pendiente |
| R5 | Registro y transmisión de logs | — | pendiente |
| R6 | Clasificación de fallos de tarea, trabajador y red | — | pendiente |
| R7 | Tratamiento del caso «la tarea terminó, pero se perdió la confirmación» | — | pendiente |
| R8 | Políticas de reintento según el tipo de fallo | — | pendiente |
| R9 | Caché basada en contenido para evitar trabajo idéntico cuando las entradas no cambian | — | pendiente |

## 3. Restricciones impuestas

No son decisiones del diseño: vienen dadas por el encargo y acotan todo lo demás.

| Restricción | Consecuencia |
|-------------|--------------|
| Lenguaje Python, framework Ray | El plano de ejecución distribuida se apoya en Ray; hay que decidir qué se delega en Ray y qué se implementa (ver §5, A1). |
| Uso de Docker | El sistema debe poder levantarse y probarse en contenedores. |
| Pruebas unitarias y CI/CD integrado | Ningún paso se da por cerrado sin pruebas y sin pasar el pipeline. |
| Código limpio y revisado | Cada cambio pasa revisión antes de integrarse; se prefiere la solución mínima que cumple el requisito. |
| Cada decisión estructural documentada en `plan/` con su porqué | Este directorio es parte del entregable, no un anexo. |
| Nivel universitario de Ciencia de la Computación | Las decisiones se justifican con el modelo de fallos y las garantías que se defienden, no solo con «funciona». |
| Commits sin coautores | Un único autor por commit: la identidad de git local. |

## 4. Decisiones tomadas en este paso

### D0.1 — Un documento numerado por paso en `plan/`

- **Decisión:** `plan/NN-titulo.md`, uno por paso, con las decisiones numeradas
  `DNN.k` y el formato *contexto → opciones consideradas → decisión → porqué →
  consecuencias*.
- **Porqué:** el orden numérico reconstruye la historia del diseño sin depender
  del historial de git, y el formato obliga a dejar escritas las alternativas
  descartadas, que es lo que se pregunta en una defensa.
- **Consecuencia:** una decisión que cambia no se reescribe: se marca
  «sustituida por DNN.k» y se añade la nueva.

### D0.2 — Documentación en español, código en inglés

- **Decisión:** `plan/` y la documentación de la entrega en español;
  identificadores, comentarios y mensajes de commit en inglés.
- **Porqué:** la asignatura y la orientación están en español; el ecosistema
  (Ray, Docker, CI) y el README existente del repo están en inglés.
- **Estado:** supuesto pendiente de confirmar por el equipo.

## 5. Decisiones abiertas

Se resuelven en los pasos siguientes; se listan aquí para que ninguna se tome
por omisión.

| ID | Pregunta | Requisitos afectados |
|----|----------|----------------------|
| A1 | ¿Qué se delega en Ray y qué se implementa? Ray ya ofrece planificación, reintentos y almacén de objetos; si se usan tal cual, R6–R8 quedarían sin demostrar. | R2, R6, R7, R8 |
| A2 | Mecanismo de aislamiento de la tarea: contenedor por tarea, o proceso con directorio de trabajo propio dentro del worker. | R3 |
| A3 | Interfaz del cliente: CLI, API HTTP o ambas; formato de la especificación del trabajo. | R1 |
| A4 | Almacén de artefactos y de la caché: clave de contenido (qué entra en el hash) y dónde vive. | R4, R9 |
| A5 | Transmisión de logs: en vivo (streaming) y/o persistidos al terminar. | R5 |
| A6 | Taxonomía de fallos y cómo se distingue cada clase con la evidencia disponible. | R6, R8 |
| A7 | Identidad de la ejecución e idempotencia para la confirmación perdida. | R7 |
| A8 | Dónde vive el estado del planificador y qué pasa si este cae. | R2, R7 |
| A9 | Framework de pruebas (`unittest` de la biblioteca estándar por defecto) y plataforma de CI/CD (GitHub Actions, por estar el repo en GitHub). | — |
| A10 | Topología de despliegue en Docker: nodos de Ray, redes y cómo se inyectan fallos para la demostración. | R6, R7 |

## 6. Riesgos conocidos

- **Ray en Windows.** El soporte de Ray en Windows es limitado; las máquinas de
  desarrollo son Windows. El clúster y las pruebas de integración deben correr
  en contenedores Linux, lo que además hace que local y CI sean el mismo entorno.
- **Solapamiento con Ray (A1).** Es el riesgo principal de evaluación: usar las
  facilidades de Ray sin modelar explícitamente los fallos deja la mitad de la
  orientación sin evidencia propia.

## Convención de esta carpeta

1. Antes de escribir código de un paso, se escribe su documento con las
   decisiones y sus alternativas.
2. Al cerrar el paso, se actualiza la tabla de trazabilidad (§2) con el paso y
   el estado, y se enlazan las pruebas que lo evidencian.
3. Las decisiones no se borran; se sustituyen dejando el rastro.
