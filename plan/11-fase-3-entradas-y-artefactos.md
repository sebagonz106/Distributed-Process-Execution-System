# 11 — Fase 3: entradas y artefactos

- **Fecha:** 2026-11-15
- **Estado:** cerrada el 2026-11-15. El corte C3 pasó en la CI de GitHub
  (ejecución 43791223473, commit `2994a70`). R3 y R4 quedan cumplidos.
- **Quién:** persona B (Sebastian), pista de datos y plataforma
- **Paso:** Fase 3 de [03-fases-y-dependencias.md](03-fases-y-dependencias.md)
- **Requisitos:** R4 (transferencia de entradas y artefactos) y el resto de R3
  (solo entra en el espacio aislado lo declarado)
- **Corte:** C3 — en el proyecto de demostración, `compile` corre en un
  trabajador y `link` en otro usando el objeto producido; el cliente descarga el
  binario y su digest coincide; una tarea que intenta leer un fichero no
  declarado no lo encuentra.

## 1. Alcance

| Feature | Qué | Estado |
|---------|-----|--------|
| F4.1 | Almacén direccionado por contenido (SHA-256) | Hecho |
| F4.2 | Subida de entradas desde el cliente | Hecho |
| F4.3 | Colocación de las entradas con verificación del digest | Hecho |
| F4.4 | Recogida de las salidas declaradas y su manifiesto | Hecho |
| F4.5 | Paso de artefactos de una tarea a sus dependientes | Hecho |
| F4.6 | Descarga de artefactos por el cliente | Hecho |
| F3.2 | Solo entra lo declarado y solo sale lo declarado | Hecho |
| F4.7 | Subida incremental (S) | No se hace (D11.9) |
| F3.6 | Límites del sistema operativo por tarea (S) | No se hace (D11.9) |

## 2. Decisiones

### D11.1 — Formato: `inputs` y `outputs`

En el fichero que escribe el usuario:

```json
{
  "tasks": {
    "compile": {
      "command": ["gcc", "-c", "main.c", "-o", "main.o"],
      "inputs": ["main.c", "greet.h"],
      "outputs": ["main.o"]
    },
    "link": {
      "command": ["gcc", "main.o", "-o", "hola"],
      "needs": ["compile"],
      "outputs": ["hola"]
    }
  }
}
```

- `inputs` es una lista de ficheros relativos a la carpeta del fichero del
  trabajo. El cliente los sube y, antes de enviar el trabajo, la convierte en
  un mapa `ruta → digest`, que es lo único que acepta la pasarela.
- `outputs` es la lista de rutas que la tarea debe producir dentro de su
  espacio de trabajo.
- **Porqué dos formas:** el usuario no tiene que calcular digests a mano, y la
  pasarela recibe algo que no depende de ningún disco: el trabajo dice
  exactamente qué contenido usa cada tarea.

### D11.2 — Rutas relativas, normalizadas y sin salir del espacio

- **Decisión:** toda ruta de `inputs` y `outputs` debe ser relativa, ya
  normalizada (`a/b.c`, no `./a//b.c`), sin `..`, sin barras invertidas y de
  hasta 255 caracteres.
- **Porqué:** es la frontera de confianza (F1.2). Una entrada `../../etc/x`
  escribiría fuera del espacio de trabajo; una salida `/etc/shadow` leería un
  fichero del trabajador. Exigir la forma normalizada evita además que dos
  textos distintos nombren el mismo fichero.

### D11.3 — Las salidas de las dependencias se colocan solas

- **Decisión:** antes de ejecutar una tarea se colocan en su espacio de trabajo
  sus `inputs` y las `outputs` de sus dependencias directas, en la misma ruta.
  Si dos de esas fuentes usan la misma ruta, el trabajo se rechaza al enviarlo.
- **Opciones:** (a) colocación automática desde `needs`; (b) que cada tarea
  nombre explícitamente qué artefacto de qué dependencia quiere.
- **Porqué (a):** `needs` ya dice de quién depende la tarea, y lo que una tarea
  declara como salida es justo lo que ofrece a quien la necesita. (b) repite
  información y abre otra forma de equivocarse. Solo dependencias directas: lo
  que una dependencia lejana produjo llega si la intermedia lo declara como
  salida suya.

### D11.4 — Digests `sha256:<hex>` y verificación en cada salto

- **Decisión:** un blob se nombra `sha256:` seguido de 64 dígitos hexadecimales
  en minúscula. Quien recibe un blob recalcula su digest antes de usarlo: la
  pasarela al subirlo, el almacén al guardarlo, el ejecutor al colocarlo y el
  cliente al descargarlo.
- **Porqué:** el digest es a la vez el nombre y la comprobación de integridad
  (S3). Verificar en cada salto hace que una corrupción en cualquier tramo se
  detecte en el siguiente, no en el resultado de la tarea.
- **Un digest que no coincide en el ejecutor** es evidencia `DIGEST_MISMATCH`
  (D06.3), clase `NETWORK` (D09.1): se reintenta.

### D11.5 — Almacén en disco del nodo de control, detrás de un actor

- **Decisión:** `dpes/cas.py` guarda cada blob como un fichero cuyo nombre es
  su digest, escribiendo primero en un temporal y renombrando, en un volumen de
  Docker del nodo de control. Un actor de Ray, fijado a ese nodo como el
  coordinador, lo sirve a la pasarela y a los ejecutores.
- **Porqué:** es D02.7. Escribir y renombrar hace que nunca exista un fichero
  con nombre de digest y contenido a medias, aunque el proceso muera a mitad.
  Como el nombre es el contenido, guardar dos veces es inofensivo. El volumen
  sobrevive a que se reinicie el contenedor (D02.4).
- **El ejecutor sigue sin conocer Ray (D07.17):** recibe un objeto con `get` y
  `put`; el envoltorio de cada intento le pasa uno que habla con el actor.
- **Tamaño máximo de un blob: 16 MiB.** Viajan enteros en memoria (límite de
  D02.7); el proyecto de demostración está muy por debajo.

### D11.6 — Solo sale lo declarado, y nunca a través de un enlace

- **Decisión:** el ejecutor recoge cada salida declarada solo si es un fichero
  regular dentro del espacio de trabajo. Un enlace simbólico, un directorio o
  algo que no existe cuenta como salida que falta: la tarea falla (`TASK`).
- **Porqué:** el ejecutor corre como `root`. Si siguiera un enlace, una tarea
  podría declarar como salida un enlace a un fichero del trabajador y llevárselo
  al almacén. Lo que la tarea dejó sin declarar no se recoge.

### D11.7 — El espacio de trabajo entero es del usuario del intento

- **Decisión:** el espacio aislado cambia de dueño el espacio de trabajo y todo
  lo que contiene (entradas incluidas) al usuario sin privilegios del intento
  (D07.16), no solo la carpeta raíz.
- **Porqué:** las entradas las escribe el ejecutor como `root`. Sin esto, una
  tarea no podría crear su salida dentro de una carpeta que llegó como entrada
  (`build/main.o` con `build/` entre las entradas).

### D11.8 — El manifiesto de artefactos va en la vista del trabajo

- **Decisión:** la vista de cada tarea gana `outputs` (ruta → digest). El
  cliente descarga con `artifacts <trabajo> <tarea> <carpeta>`, que lee esa
  vista y pide cada blob a `GET /blobs/<digest>`.
- **Diferencia con D02.6:** no se añade `GET /jobs/{id}/tasks/{t}/artifacts`;
  la vista ya tiene el manifiesto y una ruta más no aportaría nada.

### D11.9 — Lo que no se hace

- **F4.7 (subida incremental, S):** el cliente sube todas las entradas en cada
  envío. Subir un blob que ya existe es inofensivo, así que es solo coste de
  red; es la primera S en el orden de recorte (D03.6).
- **F3.6 (límites del sistema operativo por tarea, S):** sin cambios en esta
  fase.

### D11.10 — Se transfiere el contenido, no los permisos

- **Hecho:** un blob es solo bytes; un fichero colocado como entrada llega sin
  el bit de ejecución. Un binario enlazado por una tarea no se puede ejecutar
  directamente en la siguiente.
- **Decisión:** se acepta. La tarea que lo ejecuta hace `chmod +x` (la tarea de
  prueba del ejemplo lo hace). Guardar el modo cambiaría la clave de contenido
  y el contrato de las salidas.

### D11.11 — Límite: un intento matado deja su espacio de trabajo

- **Hecho:** el ejecutor borra el espacio de trabajo en un `finally`. Si el
  proceso muere de golpe (el coordinador lo cancela a la fuerza tras un plazo
  vencido, o el contenedor se cae), el `finally` no corre y la carpeta
  `/tmp/dpes-*` queda en el trabajador hasta que se reinicia el contenedor.
- **Consecuencia:** gasto de disco en el trabajador; no afecta a la corrección,
  porque cada intento crea una carpeta nueva y solo lee las suyas. Lo encontró
  la prueba de caída real, que buscaba «cualquier carpeta de trabajo» y a veces
  daba con una vieja; ahora busca una marca con nombre único.
- **Cómo se levantaría:** que el ejecutor, al arrancar en un proceso nuevo,
  borre las carpetas de procesos que ya no existen.

## 3. Pruebas

| Nivel | Fichero | Qué comprueba |
|-------|---------|---------------|
| Unitarias | `tests/unit/test_cas.py` | Digests canónicos; el almacén devuelve lo guardado, guardar dos veces es inofensivo, rechaza contenido que no coincide y nombres que no son digests |
| Unitarias | `tests/unit/test_spec.py` | `inputs` y `outputs`, cada ruta prohibida de D11.2 y una ruta que llega por dos sitios (D11.3) |
| Unitarias | `tests/unit/test_scheduler.py` | Cada intento recibe sus entradas y las salidas de sus dependencias; la vista muestra el manifiesto |
| Unitarias (Linux) | `tests/unit/test_executor.py` | Entradas colocadas y verificadas, digest que no coincide, almacén caído, salidas guardadas por digest, salida que falta, nunca a través de un enlace (D11.6), subida fallida y escritura dentro de una carpeta de entrada (D11.7) |
| Unitarias | `tests/unit/test_gateway.py` | `PUT` y `GET` de blobs, contenido que no coincide, límite de 16 MiB, entradas nunca subidas; y el cliente: subida de entradas, descarga verificada, contenido alterado y ruta que sale de la carpeta |
| Integración | `tests/integration/test_coordinator.py` | Un blob va y vuelve por el actor del almacén; un trabajo con entradas no subidas se rechaza |
| Extremo a extremo | `tests/e2e/test_artifacts.py` | **Corte C3**: el ejemplo compila y su prueba pasa; compilar en `worker-1` y enlazar en `worker-2`; el binario descargado tiene su digest y se ejecuta; un fichero no declarado no está, ni como entrada ni desde una dependencia |

Estado el 2026-11-15, en local: 174 unitarias en el contenedor (143 en
Windows), 14 de integración, 19 de extremo a extremo y 7 desde el anfitrión,
todas en verde. La de caída real falló una vez, en nueve ejecuciones, con un
error que no se volvió a reproducir en las cinco siguientes; no se identificó
la causa.

Desde Windows, con el clúster levantado:

```
py -m dpes.cli submit examples/hola/job.json
py -m dpes.cli wait <id>
py -m dpes.cli artifacts <id> link salida
```
