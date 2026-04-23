# TP Coordinación - informe

**Nombre y apellido:** Lucas Soro

**Padrón:** 95665

## Resolución de escenarios
### Un cliente, una sola réplica de cada elemento
Para este escenario, alcanzó con agregar la implementación desarrollado en TP-MOM que utiliza RabbitMQ para las abstracciones de Queues y Exchanges

### Múltiples clientes, una sola réplica de cada elemento
En esta etapa, se agregó al protocolo un client_id que se genera como un UUID desde el Gateway. Este identificador se incluye en todos los mensajes que viajan entre las instancias de todas las etapas posteriores y es imprescindible para mantener la separación adecuada del estado en cada instancia

### Múltiples clientes, sum replicado, un solo aggregation
Esta etapa fue la más dificil de resolver. La dificultad reside en la coordinación entre las instancias de Sum, donde sólo una de ellas recibe el dato de EOF. Se detalla en una sección dedicada

### Múltiples clientes, múltiples réplicas
Para este escenario, el foco estuvo puesto en evitar que las instancias de Aggregation realicen trabajo repetido, y minimizar el trabajo necesario a delegar a la instancia de Join (Lo cual es especialmente importante por ser instancia única)

### Múltiples clientes, múltiples réplicas, nombres al azar
Para este caso no hubo que realizar trabajo extra, porque en todos los casos se utilizaron las variables de entorno sin depender de particularidades del nombre, salvo la estructura "PREFIX"_"INDEX", que se respeta incluso en los nombres al azar

## Coordinación entre instancias SUM

El problema a resolver consiste en que solo una de las instancias Sum recibe el mensaje de EOF del cliente, indicando que ya no hay mas registros. Pero se necesita que todas las instancias de Sum conozcan ese mensaje, para asi poder enviar los subtotales a la etapa siguiente.

Para eso se agregó un exchange de control, de manera que cuando la instancia Sum recibe un mensaje de EOF del cliente, la misma envía un mensaje de tipo EOF_NOTIFY a la exchange de control, a la cual las demas instancias Sum estan suscritas. Luego cada instancia que reciba ese mensaje al consumir del exchange, envía los subtotales a las instancias de Aggregation que corresponda (Se detalla la distribución en una sección dedicada).

Sin embargo, por la forma en que el middleware distribuye las tareas, con este esquema podía ocurrir que para una instancia de Sum se procese el mensaje EOF_NOTIFY antes de que se procesaran los mensajes de datos en espera en la cola de entrada, debido al prefetch, donde cada instancia tiene localmente un buffer asociado al canal con mensajes asignados para su procesamiento.

Para resolver este problema, se optó por asignar un prefetch de 1 mensaje para la cola de entrada, entendiendo que esta decisión acarrea un impacto negativo notable en performance, que empeora cuanto mayor sea la latencia entre la instancia de RabbitMQ y las instancias de Sum. Con esta solución, solo queda preocuparse por un único posible mensaje de datos en la cola de entrada, pero este inconveniente se resolvió utilizando un lock que impida procesar un mensaje de EOF_NOTIFY mientras se procesan otros datos.

### Posible mejora

(No implementada) Para mitigar el impacto de performance de esta solución, en vez de asignar un prefetch de 1, se puede incrementar a un N adecuado. El flujo consiste en señalizar con un flag por cliente el estado de EOF al recibir un EOF_NOTIFY. Luego se espera el procesamiento de N mensajes (Cualquiera de la cola de entrada, no es necesario que sean del mismo cliente, lo importante es que se haya barrido la ventana de prefetch), y luego proceder al envío de totales a las instancias de Aggregation.

Además, se debe tener en cuenta el escenario donde ya no haya más mensajes a recibir. Este caso puede resolverse mediante un Watch dog, que consiste en tener un hilo que gestiona un timeout, que al no recibir mensajes en un tiempo determinado, activa la transmisión de mensajes hacia los Aggregators, siempre y cuando el flag de EOF esté activado para el cliente.

**Cómo determinar el N correcto:** Para ello, hay que hacer profiling. Primero se debe determinar la velocidad de procesamiento de la instancia Sum. Al saber el tiempo promedio de procesamiento de un mensaje en milisegundos, se sabe que N debe ser mayor a la cantidad de paquetes que puede procesarse en el tiempo dado por la latencia entre RabbitMQ y Sum

### Otras soluciones exploradas

En la búsqueda de una solución adecuada, se exploraron otras opciones que fueron descartadas por su fragilidad.

**Regreso a input queue:** Consiste en que cada instancia de Sum, al recibir el EOF, reingrese el mensaje incrementando en 1 un contador de visitas, si es la primera vez que se recibe el EOF del cliente, a la vez que se entregan los subtotales a los Aggregators. Cuando se recibe un EOF cuyo contador coincide con el total de instancias Sum, se deja de reingresar el mensaje. Se descartó porque la aleatoriedad de este esquema puede incrementar enormemente los tiempos de procesamiento totales por cliente, además de generar tráfico adicional de mensajes

**Canales de RabbitMQ compartidos:** Por la forma en que el middleware internamente gestiona los canales, es posible vincular la input queue y el exchange de control al mismo canal, compartiendo la conexión. De esa forma, al recibir un EOF_NOTIFY, este solo se procesará luego de los mensajes presentes en el buffer de prefetch. Esta opción tendría buenos resultados, pero se descartó por requerir cambios en la interfaz dada que rompen con la abstracción y el encapsulamiento, al depender de detalles de implementación de RabbitMQ

## Coordinación entre instancias Sum y Aggregation

Para eliminar la repetición de trabajo de las instancias Aggregation, se hizo una división por nombre de frutas. De esta manera, se pueden generar subtotales y tops parciales, garantizando la no intersección con las demaás instancias. Para ello, en las instancias de Sum se implementó un distribuidor de mensajes de subtotales, donde cada fruta se envía a una instancia u otra de Aggregation según el resultado de un Hash sobre el nombre de la fruta. La función de hash esta hecha a medida, para tener buena performance y aprovechar todas las instancias que sean posibles. Para esto se hace un cálculo en base al valor de cada caracter y su posición en el string.

## Uso de threads vs procesos

El sistema de threads en Python está limitado por el GIL (Global Interpreter Lock), que impide la ejecución simultánea de bytecodes en más de un thread. En el caso de operaciones de calculo intensivo, es una desventaja que motiva fuertemente al uso de procesos para beneficiarse de paralelismo real (Cuando el entorno lo permite). Sin embargo este no es el caso, ya que las operaciones a realizar son simples y el cuello de botella son las esperas debido a usar conexiones bloqueantes con el middleware. Por lo tanto, el uso de threads en vez de procesos sigue siendo una opción razonable para este caso.

## Escalabilidad

Esta solución es escalable respecto a la cantidad de clientes, asi como instancias asignadas a cada etapa. Al aumentar la cantidad de clientes, se debe tener en cuenta que cada instancia guarda temporalmente un estado para ese cliente, que se descarta al terminar las operaciones. Por ello, si la cantidad es extremadamente grande, se puede evaluar un volvado selectivo a disco en caso de que haya picos muy altos de consumo de memoria.

Para la cantidad de instancias por etapa, los tiempos de procesamiento mejoran al aumentar la cantidad de instacias, siempre que se mantenga en valores razonables. Un ejemplo donde esto no se cumple, es cuando se tienen muchas más instancias de Sum o Aggregation que los Fruit_records a procesar. En ese caso no se obtienen mejoras al escalar, por el contrario, el overhead de comunicación puede generar congestioón en la red que empeore los tiempos. Si este caso puede llegar a ocurrir en el uso normal, sería conveniente implementar alguna técnica de clustering por cliente.
