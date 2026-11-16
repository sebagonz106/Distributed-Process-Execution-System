# Progreso

**Entrega:** martes 1 dic 2026 · **Actualizado:** 16 nov 2026

✅ hecho · 🟡 en curso · ⬜ pendiente

## Avance

`███████░░░` **7 de 10** — plan aprobado, 6 de 9 fases cerradas

## Fases

| | Fase | Está cerrada cuando… | Requisitos | Quién | Cierre |
|:-:|------|----------------------|:----------:|:-----:|:------:|
| ✅ | Plan | Análisis, arquitectura, fases, calendario y diagramas aprobados | — | A | 14 nov |
| ✅ | 0 · Cimientos | Clúster de 3 nodos, CI en verde, experimento de fallos de Ray | — | A y B | 15 nov |
| ✅ | 1 · Una tarea | Se envía una tarea, corre aislada, se ve su estado y su salida | R1 R3 R5 | A y B | 15 nov |
| ✅ | 2 · DAG y recursos | Rombo en paralelo, ciclo rechazado, dependientes omitidos | R1 R2 | A | 15 nov |
| ✅ | 3 · Artefactos | Compila en un nodo, enlaza en otro, se descarga el binario | R3 R4 | B | 15 nov |
| ⬜ | 4 · Logs | `logs -f` en vivo; el log sobrevive al trabajador muerto | R5 | B | 24 nov |
| ✅ | 5 · Fallos | Un guion por clase de fallo, con su reintento | R6 R8 | A | 15 nov |
| ✅ | 6 · Confirmación | El trabajador muere tras guardar: una sola ejecución | R7 | A | 16 nov |
| ⬜ | 7 · Caché | Reenviar no ejecuta nada; cambiar un fichero recompila solo ese | R9 | B | 28 nov |
| ⬜ | 8 · Entrega | Demo desde un clon limpio, imagen publicada | — | A y B | 30 nov |

## Requisitos

| | R1 | R2 | R3 | R4 | R5 | R6 | R7 | R8 | R9 |
|---|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|
| Estado | ✅ | ✅ | ✅ | ✅ | ⬜ | ✅ | ✅ | ✅ | ⬜ |
| Se cumple al cerrar la fase | 2 | 2 | 3 | 3 | 4 | 5 | 6 | 5 | 7 |

## Transversal

| | Qué | Quién |
|:-:|-----|:-----:|
| ✅ | Sistema de logs descriptivo, con la traza de un trabajo verificada en la CI ([plan/10](plan/10-sistema-de-logs.md)) | A |
| ✅ | Trazas de lo construido después: artefactos, almacén de resultados e idempotencia ([plan/10 §2.1](plan/10-sistema-de-logs.md)) | B |

## Siguiente: 4 · Logs (B) y 7 · Caché (B), después 8 · Entrega (A y B)

Las dos de B pueden empezar ya. La Fase 6 tocó dos piezas de su pista, el
ejecutor y el actor del almacén ([plan/12](plan/12-fase-6-confirmacion-perdida.md));
la 7 usará los resultados confirmados que guarda ahora el almacén.
Pruebas hoy: 204 unitarias, 17 de integración, 22 de extremo a extremo y 8
desde el anfitrión, en verde.

Los contratos de `dpes/messages.py` quedaron congelados al cerrar la Fase 1
sin que B los revisara; cambiarlos ahora requiere una decisión escrita
([plan/07](plan/07-fase-1-una-tarea.md)).

Hallazgo de la Fase 0: con un trabajador pausado o sin red, Ray no avisa en
3 minutos; el plazo propio del coordinador es la única señal
([plan/06](plan/06-fase-0-cimientos.md), D06.14).

---

Para actualizar: cambiar el icono de la fila, un bloque de la barra y la fecha.
El detalle de cada fase está en
[plan/03-fases-y-dependencias.md](plan/03-fases-y-dependencias.md) y el
calendario en [plan/04-plan-de-implementacion.md](plan/04-plan-de-implementacion.md).
