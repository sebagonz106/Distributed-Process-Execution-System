# 04 — Plan de implementación

- **Fecha:** 2026-11-14
- **Estado:** aprobado el 2026-11-14
- **Paso:** planificación
- **Entrega:** martes 1 de diciembre de 2026

Quién hace qué y cuándo. Las fases y sus cortes (`C0`–`C8`) están definidos en
[03-fases-y-dependencias.md](03-fases-y-dependencias.md).

## 1. Decisiones de organización

### D04.1 — Reparto por pistas, no por capas ni por requisitos

- **Opciones:** (a) repartir requisitos alternos; (b) una persona el servidor y
  otra el cliente; (c) dos pistas que siguen las dos cadenas del mapa de
  dependencias.
- **Decisión:** (c).
  - **Persona A — pista de control:** especificación, grafo, coordinador,
    clasificación de fallos, reintentos, confirmación. Es el camino crítico
    (0 → 1 → 2 → 5 → 6 → 8).
  - **Persona B — pista de datos y plataforma:** contenedores, integración y
    entrega continuas, espacio aislado y ejecutor, almacén, logs, caché,
    herramientas de inyección de fallos (0 → 1 → 3 → 4 → 7 → 8).
- **Porqué:** las dos pistas tocan módulos distintos (D02.16), así que apenas
  hay conflictos al fusionar, y se encuentran solo en los contratos fijados en
  la Fase 0 (D03.3). Con (a) las dos personas editarían el coordinador a la vez.
  Con (b) el cliente es trivial y la carga queda descompensada.
- **Pendiente del equipo:** decidir quién es A y quién es B.

### D04.2 — Supuesto de dedicación

- **Supuesto:** unas 4 horas efectivas por persona y día, incluidos fines de
  semana, del 15 al 30 de noviembre: 16 días, unas 128 horas en total.
- **Porqué se escribe:** el calendario de §2 solo es creíble bajo este supuesto.
  Si la dedicación real es menor, no se alarga el calendario: se aplica la línea
  de corte (D03.6) en los puntos de control.

### D04.3 — Ramas cortas con revisión cruzada

- **Opciones:** (a) todo directo a la rama principal; (b) una rama por feature o
  grupo pequeño de features, fusionada con solicitud de cambio revisada por la
  otra persona y con la integración continua en verde.
- **Decisión:** (b) para el código. La vida de una rama no supera un día.
- **Porqué:** «código revisado» es una restricción del encargo, y con dos
  personas la revisión natural es la del otro. Ramas de un día mantienen la
  integración continua de verdad y hacen que cada revisión sea pequeña.
- **Nota:** la documentación de `plan/` de este paso se sube directamente a la
  rama principal, como se ha venido haciendo.

### D04.4 — Definición de «hecho»

Una feature está hecha cuando se cumplen las cinco:

1. Sus decisiones están escritas en el documento de su fase.
2. Tiene pruebas unitarias que fallarían si la feature se rompiera.
3. La integración continua pasa.
4. La otra persona revisó el cambio.
5. La tabla de trazabilidad de `plan/00` está actualizada.

Una fase está cerrada cuando su corte pasa como guion en la integración
continua (D03.4).

### D04.5 — Un documento de `plan/` por fase

- **Decisión:** al empezar cada fase se crea su documento con el siguiente
  número libre (`05-fase-0-…`, `06-fase-1-…`). Recoge las decisiones de
  implementación que este plan deja abiertas a propósito: nombres de tablas,
  formato exacto de los mensajes, valores de plazos y tamaños.
- **Porqué:** esas decisiones dependen de lo que se aprende al construir;
  tomarlas hoy sería inventarlas.

### D04.6 — Margen y congelación

- **Decisión:** el código se congela al final del domingo 29. El lunes 30 solo
  admite correcciones de fallos encontrados al ensayar. El martes 1 se entrega;
  no se programa.
- **Porqué:** el camino crítico no tiene holgura. El margen se pone al final y
  se protege con una regla, porque un margen sin regla se gasta en features.

## 2. Calendario

| Fecha | Persona A — control | Persona B — datos y plataforma | Corte |
|-------|---------------------|--------------------------------|:-----:|
| Dom 15 nov | **F0.** Estructura del paquete, contratos entre componentes, andamiaje de pruebas | **F0.** Imagen, composición con control y dos trabajadores, integración continua (unitarias + imagen) | |
| Lun 16 nov | **F0.** Experimento de fallos de Ray y su tabla (con B) | **F0.** Comandos para matar, pausar y desconectar un trabajador; experimento (con A) | **C0** |
| Mar 17 nov | **F1.** Especificación de una tarea, pasarela (enviar y consultar), cliente | **F1.** Espacio aislado: directorio, entorno, grupo de procesos, tiempo límite | |
| Mié 18 nov | **F1.** Coordinador mínimo y estado en disco; integración con B | **F1.** Ejecutor como tarea de Ray, captura de salida; integración con A | **C1** |
| Jue 19 nov | **F2.** Validación completa, grafo, ciclos | **F3.** Almacén por contenido, subida y bajada de blobs | |
| Vie 20 nov | **F2.** Máquina de estados pura: tareas listas, propagación de fallos | **F3.** Subida de un directorio desde el cliente, colocación de entradas | |
| Sáb 21 nov | **F2.** Recursos, paralelismo, límite en vuelo; guion del rombo | **F3.** Recogida de salidas, artefactos entre tareas, descarga | **C2** |
| Dom 22 nov | Integración de F2 con F3, revisión cruzada. **Punto de control 1** | **F3.** Proyecto de demostración: compilar y enlazar; límites del sistema | **C3** |
| Lun 23 nov | **F5.** Taxonomía y clasificador puro | **F4.** Fragmentos numerados, registro de logs, lectura por desplazamiento | |
| Mar 24 nov | **F5.** Política de reintentos y esperas en la máquina de estados | **F4.** Seguimiento en vivo, historial de eventos | **C4** |
| Mié 25 nov | **F5.** Plazos por intento, `FAILED` frente a `ERROR`; guiones de fallos | **F5 (apoyo).** Puntos de fallo en el ejecutor, guiones de fallos en la integración continua | **C5** |
| Jue 26 nov | **F6.** Identidad de ejecución, guardado «primero gana», reconciliación. **Punto de control 2** | **F7.** Clave de acción y sus pruebas | |
| Vie 27 nov | **F6.** Reintento del guardado, confirmaciones tardías y duplicadas, envío idempotente | **F7.** Consulta y escritura de caché, claves encadenadas, exclusión por tarea | **C6** |
| Sáb 28 nov | **F8.** Recuperación del coordinador; todos los guiones de fallos en la integración continua | **F7.** Cierre. **F8.** Publicación de la imagen | **C7** |
| Dom 29 nov | **F8.** Comparación arquitectónica, repaso final de `plan/`. **Congelación** | **F8.** Guía de uso, demostración desde un clon limpio. **Congelación** | |
| Lun 30 nov | Ensayo de la demostración; solo correcciones; versión etiquetada | Ensayo de la demostración; solo correcciones | **C8** |
| Mar 1 dic | **Entrega** | **Entrega** | |

Cada día empieza con una puesta en común de diez minutos: qué se cerró, qué
bloquea, si algún contrato necesita cambiar.

### Días de integración obligada

Son los días en que las dos pistas se tocan; conviene trabajar a la vez.

| Día | Qué se integra |
|-----|----------------|
| Lun 16 | El experimento necesita el clúster de B y el guion de A |
| Mié 18 | Coordinador de A con ejecutor de B: primer extremo a extremo |
| Dom 22 | Planificador de A con artefactos de B: una tarea usa lo que produjo otra |
| Mié 25 | Clasificador de A con los puntos de fallo de B |
| Vie 27 | Guardado de resultado de A con el índice de caché de B |

## 3. Puntos de control

| Fecha | Debe estar cerrado | Si no lo está |
|-------|--------------------|---------------|
| Dom 22 nov | C0, C1, C2, C3 | Se eliminan todos los C y los cuatro primeros S de D03.6 |
| Jue 26 nov | C4, C5 | Se eliminan todos los S; la Fase 8 se reduce a entrega y documentación |
| Dom 29 nov | C6, C7 | El lunes 30 se dedica a cerrar el que falte; no se añade nada más |

## 4. Riesgos

| Riesgo | Probabilidad | Efecto | Respuesta |
|--------|:------------:|--------|-----------|
| Lo que Ray deja ver de un fallo no coincide con lo supuesto en D02.9 | Media | Rehacer la clasificación | Experimento el día 2 (D03.2); la taxonomía se ajusta antes de construir sobre ella |
| Ray no funciona bien en las máquinas de desarrollo (Windows) | Alta | No se puede probar en local | Todo lo que usa Ray corre en contenedores; las pruebas puras no lo necesitan (D02.13) |
| Las pruebas de fallos son inestables por depender de tiempos | Alta | Integración continua que falla sin motivo | Puntos de fallo deterministas dentro del ejecutor (D02.15); los fallos desde fuera se dejan para la demostración |
| La integración continua tarda demasiado al levantar el clúster | Media | Ciclo de revisión lento | Las unitarias puras van primero y cortan pronto; los guiones de extremo a extremo solo en la rama principal y en solicitudes de cambio |
| Una persona no está disponible varios días | Media | El camino crítico se detiene | Los contratos (D03.3) permiten que la otra continúe con dobles; se aplica la línea de corte |
| Aparece alcance nuevo a mitad | Media | Se come el margen | La regla de `CLAUDE.md`: nada fuera de R1–R9 sin pedirlo; lo nuevo se anota y se decide en un punto de control |
| El espacio aislado solo funciona en Linux | Segura | Sus pruebas no corren en Windows | Se marcan para saltarse fuera de Linux y se ejecutan en contenedor |
| El límite de tamaño de los blobs se queda corto para la demostración | Baja | El ejemplo no cabe | El proyecto de demostración es pequeño a propósito (D01.7) |

## 5. Aprobaciones

El plan se aprobó tal como estaba el 2026-11-14. Queda un único asunto abierto.

| Asunto | Resolución | Dónde se decide |
|--------|------------|-----------------|
| Enfoque general: Ray como sustrato y plano de control propio | Aprobado | D02.1 |
| Aislamiento por proceso en lugar de contenedor por tarea | Aprobado, con el límite escrito | D02.5 |
| `gcc` y `make` en la imagen de trabajadores | Aprobado; son paquetes del sistema, no librerías de Python | D01.7 |
| Acciones de GitHub de terceros en la integración continua (obtener el código, instalar Python, construir y publicar la imagen) | Aprobado: solo las oficiales de GitHub y de Docker | D02.14 |
| Analizador de estilo y medición de cobertura | No se usan | D02.13 |
| Ramas con revisión cruzada para el código | Aprobado | D04.3 |
| Quién es la persona A y quién la B | **Abierto:** lo decide el equipo | D04.1 |
