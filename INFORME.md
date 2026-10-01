# Trabajo práctico individual - Coordinación

**Retamozo Melina, 110065**

En este trabajo se presentó una limitación respecto de los archivos cuya edición estaba permitida. A continuación, se detallan las modificaciones realizadas en cada uno de ellos.

## Message Handler

El `MessageHandler` es el encargado de serializar los mensajes enviados por el Gateway hacia las instancias de Sum y de deserializar los mensajes recibidos desde Join. Su principal responsabilidad es mantener un identificador asociado a cada cliente, de manera que cada registro recibido pueda asociarse correctamente con el cliente correspondiente y posteriormente ser procesado de forma independiente.

## Protocolo interno

Para la serialización de mensajes entre los servicios internos del servidor se utilizó un protocolo TLV sobre streams de bytes. Para esto se definió un enum `FieldType` con los siguientes valores:

```python
class FieldType(Enum):

    EndOfMessage = 0

    FruitName = 1

    FruitAmount = 2

    Uuid = 3
```

El Gateway recibe del cliente pares fruta-cantidad, que luego reenvía a las instancias de Sum agregando el UUID del cliente. Para serializar los mensajes enviados desde el Gateway hacia las distintas instancias de Sum se utiliza el método `serialize(message)`, que recibe los tres valores mencionados.

Cuando el cliente finaliza el envío de sus registros, el Gateway envía un mensaje que contiene únicamente el UUID del cliente. De esta forma, las instancias de Sum pueden identificar que se ha terminado de recibir información para dicho cliente.

El formato del stream de bytes es el siguiente:

```text
TOTAL LENGTH | UUID (TLV) | FRUIT_NAME (TLV) | FRUIT_AMOUNT (TLV)
```

En el caso de un mensaje de finalización, se envían únicamente el `total_length` y el UUID:

```text
TOTAL LENGTH | UUID (TLV)
```

Para la comunicación entre Sum y Aggregation se utiliza la misma lógica de serialización.

Para la comunicación entre Aggregation y Join se utiliza el método `serialize_top_message(message)`, que recibe el UUID del cliente seguido de una lista de `FruitItem`. En este caso, el mensaje se serializa con el siguiente formato:

```text
TOTAL_LENGTH | UUID (TLV) | [ FRUIT ITEM (TLV) ]
```

Este mismo formato se utiliza para la comunicación entre Join y Gateway.

## Sum

Las distintas instancias de Sum consumen una misma cola de entrada. RabbitMQ distribuye los mensajes entre las réplicas, por lo que cada instancia recibe una parte de los registros enviados por los clientes.

Para evitar mezclar información perteneciente a distintos clientes, el acumulador dejó de estar indexado únicamente por fruta y pasó a estar indexado por UUID y fruta. De esta forma, los registros de cada cliente se procesan de manera independiente, incluso cuando existen múltiples clientes concurrentes.

Al recibir un EOF, representado mediante un mensaje que contiene únicamente el UUID del cliente, cada instancia de Sum envía los totales acumulados al Aggregation correspondiente. La elección del Aggregation se realiza mediante un hash determinista de `uuid + fruta`. De esta forma, una misma fruta perteneciente a un mismo cliente siempre es enviada al mismo Aggregation, independientemente de qué réplica de Sum haya procesado originalmente el registro.

Esto permite distribuir el procesamiento entre las distintas instancias de Aggregation sin necesidad de realizar un broadcast de los datos.

Debido a que el Gateway envía el EOF únicamente a una de las réplicas de Sum, se implementó un mecanismo de comunicación entre todas las réplicas para propagar dicho EOF. La réplica que recibe el EOF lo reenvía a la siguiente instancia de Sum, utilizando los identificadores configurados mediante variables de entorno. De esta manera, todas las réplicas reciben la notificación de finalización de los registros de un determinado cliente y pueden enviar al Aggregation los conteos que hayan acumulado.

Para implementar este mecanismo se utilizó un `MessageMiddlewareExchangeRabbitMQ`. Cada réplica se suscribe a un tópico asociado a su propio ID y registra el exchange correspondiente al siguiente nodo. Al iniciarse, cada instancia comienza a escuchar el tópico asociado a su ID. Cuando recibe un mensaje de EOF, lo reenvía utilizando como routing key el ID de la siguiente instancia.

El procesamiento de datos y el procesamiento de los EOF se ejecutan en hilos diferentes. Por este motivo, se utiliza un lock para proteger tanto el acceso al acumulador como el uso de los exchanges de salida. Esto es necesario debido a que las conexiones de Pika no son thread-safe y no deben utilizarse simultáneamente desde distintos callbacks.

## Aggregation

Cada instancia de Aggregation se suscribe exclusivamente al tópico asociado a su ID, configurado mediante una variable de entorno. Debido a la forma en que se implementó el hash en Sum, una determinada fruta de un determinado cliente siempre será enviada al mismo Aggregation.

Sin embargo, una misma instancia de Aggregation puede recibir registros correspondientes a distintos clientes. Por este motivo, los registros se acumulan de forma independiente para cada UUID.

Además, un mismo cliente puede haber enviado registros que hayan sido procesados por distintas réplicas de Sum. Por lo tanto, antes de enviar el top al Join, cada Aggregation debe esperar a que todas las réplicas de Sum hayan enviado sus registros correspondientes a ese cliente.

Para esto, cada instancia lleva un registro de la cantidad de EOF recibidos para cada cliente y espera hasta alcanzar el valor de `SUM_AMOUNT`. Una vez recibidos todos los EOF correspondientes, calcula el top parcial y lo envía al Join.

Una vez emitido el resultado, los acumuladores asociados al cliente se eliminan para liberar memoria y permitir el procesamiento de nuevas consultas.

## Join

Al igual que ocurre con las distintas réplicas de Sum, un mismo cliente puede tener información procesada por distintas instancias de Aggregation. Por lo tanto, el Join debe esperar a recibir los resultados parciales de todas ellas antes de generar el resultado final.

Para garantizar esto, el Join mantiene un registro de los EOF recibidos desde cada instancia de Aggregation para cada cliente. Solo cuando se han recibido `AGGREGATION_AMOUNT` resultados parciales para un determinado UUID, el Join genera y envía el resultado final al Gateway.