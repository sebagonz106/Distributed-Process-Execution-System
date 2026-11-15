# Progreso

**Entrega:** martes 1 dic 2026 · **Actualizado:** 15 nov 2026

✅ hecho · 🟡 en curso · ⬜ pendiente

## Avance

`██░░░░░░░░` **2 de 10** — plan aprobado, 1 de 9 fases cerradas

## Fases

| | Fase | Está cerrada cuando… | Requisitos | Quién | Cierre |
|:-:|------|----------------------|:----------:|:-----:|:------:|
| ✅ | Plan | Análisis, arquitectura, fases, calendario y diagramas aprobados | — | A | 14 nov |
| ✅ | 0 · Cimientos | Clúster de 3 nodos, CI en verde, experimento de fallos de Ray | — | A y B | 15 nov |
| 🟡 | 1 · Una tarea | Se envía una tarea, corre aislada, se ve su estado y su salida | R1 R3 R5 | A y B | 18 nov |
| ⬜ | 2 · DAG y recursos | Rombo en paralelo, ciclo rechazado, dependientes omitidos | R1 R2 | A | 21 nov |
| ⬜ | 3 · Artefactos | Compila en un nodo, enlaza en otro, se descarga el binario | R3 R4 | B | 22 nov |
| ⬜ | 4 · Logs | `logs -f` en vivo; el log sobrevive al trabajador muerto | R5 | B | 24 nov |
| ⬜ | 5 · Fallos | Un guion por clase de fallo, con su reintento | R6 R8 | A | 25 nov |
| ⬜ | 6 · Confirmación | El trabajador muere tras guardar: una sola ejecución | R7 | A | 27 nov |
| ⬜ | 7 · Caché | Reenviar no ejecuta nada; cambiar un fichero recompila solo ese | R9 | B | 28 nov |
| ⬜ | 8 · Entrega | Demo desde un clon limpio, imagen publicada | — | A y B | 30 nov |

## Requisitos

| | R1 | R2 | R3 | R4 | R5 | R6 | R7 | R8 | R9 |
|---|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|
| Estado | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |
| Se cumple al cerrar la fase | 2 | 2 | 3 | 3 | 4 | 5 | 6 | 5 | 7 |

## Fase en curso: 1 · Una tarea

| | Parte | Quién |
|:-:|-------|:-----:|
| ✅ | Especificación de una tarea y su validación | A |
| ✅ | Máquina de estados mínima | A |
| ✅ | Pasarela HTTP y cliente (`submit`, `status`, `wait`) | A |
| ✅ | Coordinador como actor de Ray (probado con un ejecutor de prueba) | A |
| ✅ | Espacio aislado y ejecutor | A, por B |
| ✅ | Pasarela en el clúster y prueba C1 (en local) | A |
| ⬜ | CI en verde en GitHub (falta el push) | — |

Pruebas: 69 unitarias, 6 de integración y 6 de extremo a extremo en verde.
Decisiones: [plan/07-fase-1-una-tarea.md](plan/07-fase-1-una-tarea.md).

Pendiente: que B acepte los contratos de `dpes/messages.py` antes de que se
congelen al cerrar esta fase.

Hallazgo de la Fase 0: con un trabajador pausado o sin red, Ray no avisa en
3 minutos; el plazo propio del coordinador es la única señal
([plan/06](plan/06-fase-0-cimientos.md), D06.14).

---

Para actualizar: cambiar el icono de la fila, un bloque de la barra y la fecha.
El detalle de cada fase está en
[plan/03-fases-y-dependencias.md](plan/03-fases-y-dependencias.md) y el
calendario en [plan/04-plan-de-implementacion.md](plan/04-plan-de-implementacion.md).
