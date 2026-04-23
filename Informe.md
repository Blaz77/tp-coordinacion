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
Para este caso no hubo que realizar trabajo extra, porque en todos los casos se utilizaron las variables de entorno sin depender de particularidades del nombre, salvo la estructura <PREFIX>_<INDEX>, que se respeta incluso en los nombres al azar

## Coordinación entre instancias SUM
