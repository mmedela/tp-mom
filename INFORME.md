# Informe de diseño - Middleware RabbitMQ (python)

## 1. Alcance

Implementacion completa de `MessageMiddlewareQueueRabbitMQ` y `MessageMiddlewareExchangeRabbitMQ` en `python/src/common/middleware/middleware_rabbitmq.py`, sobre la interfaz probista en `python/src/common/middleware/middleware.py` usando la libreria `pika` sobre RabbitMQ

## 2. Decisiones de diseño 

### 2.1 Tipo de exchange: `direct`

Se eligio `exchange_type='direct'` en lugar de danout o topic porque en los tests se contemplaban 2 escenarios:
- **Single producer, single consumer**: un routing key mapea a un unico consumidor
- **Single producer, multiple consumers**: es basicamente un broadcast donde el mismo routing key es escuchado por varios consumidores. Hay varias colas asociadas a la misma key que reciben todas el mismo mensaje.

`direct` cubre ambos casos segun cuantas coas se bindeen a la misma key, mientras que `fanout` ignoraria el routing key (no permitiendo distinguir consumidores por key) y `topic` fuerza a usar logica de patrones uqe agrega complegidad innecesaria para los casos de uso pedidos.

### 2.2 Cola propia por consumidor en el exchange

Cada instancia de `MessageMiddlewareExchangeRabbitMQ` declara su propia cola exclusiva y anonima (`queue_declare(queue='', exclusive=True)`), asociada al routing key recibido. Esto se hace para poder respetar fielmente el patron publicador-suscriptor, donde cada suscriptor recibe su propia copia de los mensajes que le corresponden, sin competor por mensajes con otros suscriptores salvo que comparta el routing key.

### 2.3 Adaptacion del callback de pika a la intergaz del middleware

La interfaz define un callback `(message, ack ,nack)`, mientras que pika entrega `(channel, method, properties, body)` en `basic_consume`. Se resuelve envolviendo el callback de pika y construyendo `ack`/`nack` como funciones que hacen `basic_ack`/`basic_nack` sobre el `delivery_tag` del mensaje en curso, delegando el ack manual explicitamente al usuario del middleware con `auto_ack=False` (comportamiento por default en `pika 1.4.4`)

### 2.4 `stop_consuming` desde dentro del propio callback

El contrato de la interfaz permite invocar `stop_consuming()` desde dentro del callback de `on_message_callback`, en el mismo thread que esta bloqueado por `channel.start_consuming()`. Esto se resuelve llamado a `channel.stop_consuming()`, que segun la documentacion del paquete, es seguro de llamar desde ese contexto y corta el loop de consumo sin bloquear

### 2.5 Encapsulamiento de errores de RabbitMQ

Ningun tipo de `pika` se filtra fuera de `middleware_rabbitmq.py`. Las excepciones de  pika se capturan y traducen a las propias del dominio: `AMQPConnectionError`, `ChannelWrongStateError`, `ConnectionError`, `OSError` se traducen a `MessageMiddlewareDisconnectedError` y cualquier otro `AMQPError` (errores de protocolo no resolubles) se traduce a `MessageMiddlewareMessageError`

Esto se aplica en `send`, `start_consuming`, `close` y `stop_consuming`

### 2.6 Composicion sobre la herencia para compartir `start_consuming`, `close` y `stop_consuming`

Estos tres metodos son practicamente identicos en ambos en `MessageMiddlewareQueueRabbitMQ` y `MessageMiddlewareExchangeRabbitMQ`: Ambos operan sobre un `channel` o `connection` de pika y traducen las mismas excepciones en vez de duplicar ese codigo o resolverlo con herencia multiple (mixin), se extrajo a una clase colaboradora `_RabbitMQMiddlewareBase`, que cada clase concreta **instancia y usa por composicion**, delegandelo esos tres metodos.

Se pregirio composiion sobre herencia por decision de la catedra segun lo discutido en el foro de consultas de la materia

`send` no se delego porque su logica difiere entre ambas clases. Una publica a la cola default y la otra itera sobre multiples routing keys de un exchange, por lo que cada clase contreta mantiene su propia implementacion.

## 3 Trade-oofs /limitaciones conocidas

- No se usan publishers comfirms, porlo que `send` no garantiza que el mensaje haya sido efectivamente ruteado por el broker, solo detecta errores de conexion o de protocolo al momento de publicar.

- `send` sigue duplicando el mismo bloque de traduccion de excepciones de ambas clases concretas, no se delego a `_RabbitMQMiddlewareBase` por la diferencia en logia mencionada en 2.6. Queda como duplicacion menor aceptada a cambio de mantener cada `send` simple y explicito

## 4 Validacion

Se ejecuto la bateria de pruebas provista con `make test`. Pasan 19 de 19, cubriendo escenarios de uno y varios productores-consumidores en work queues, no mezcla mensajes entre colas, direct messaging y broadcast sobre exchanges.