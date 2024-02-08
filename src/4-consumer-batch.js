const  kafka  = require('./config/kafkaConfig');
const logger = require('./utils/logger');

const topic = 'my-topic';
const consumer = kafka.consumer({ groupId: 'my-group' });

/**
 * eachBatch
 * 
 * Algunos casos de uso requieren tratar con lotes directamente. Este gestor alimentará los lotes de la función y 
 * proporcionará algunas funciones de utilidad para dar más flexibilidad al código: resolveOffset, heartbeat, 
 * commitOffsetsIfNecessary, uncommittedOffsets, isRunning, isStale y pause. 
 * Todos los offsets resueltos se consignarán automáticamente tras la ejecución de la función.
 * 
 * NOTA: Tenga en cuenta que el uso de eachBatch directamente se considera un caso de uso 
 * más avanzado en comparación con el uso de eachMessage, ya que tendrá que entender 
 * cómo se conectan los tiempos de espera de sesión y los latidos del corazón.
 * 
 * 
 * consumer.run({
    eachBatchAutoResolve: false,
    eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
        for (let message of batch.messages) {
            if (!isRunning() || isStale()) break
            await processMessage(message)
            resolveOffset(message.offset)
            await heartbeat()
        }
    }
})
 * En el ejemplo anterior, si el consumidor se apaga en mitad del lote, los mensajes restantes no 
se resolverán y, por tanto, no se consignarán. De esta forma, puedes cerrar rápidamente el consumidor 
sin perder/omitir ningún mensaje. Si el lote se queda obsoleto por alguna otra razón 
(como llamar a consumer.seek), ninguno de los mensajes restantes se procesará.
 * 
 */


const run = async () => {
    // Consuming
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });
    await consumer.run({
        eachBatchAutoResolve: true, // configura la auto-resolución del procesamiento por lotes. Si se establece en true, KafkaJS consignará automáticamente el último desplazamiento del lote si eachBatch no arroja un error. Por defecto: true
        eachBatch: async ({
            batch,
            resolveOffset,
            heartbeat,
            commitOffsetsIfNecessary, //  se utiliza para confirmar desplazamientos en función de las configuraciones de autoCommit (autoCommitInterval y autoCommitThreshold). Tenga en cuenta que la confirmación automática no se producirá en eachBatch si no se invoca commitOffsetsIfNecessary. Echa un vistazo a autoCommit para más información
            uncommittedOffsets, // returns all offsets by topic-partition which have not yet been committed
            isRunning, // devuelve true si el consumidor está en estado de ejecución, en caso contrario devuelve false
            isStale, // devuelve si los mensajes del lote se han vuelto obsoletos por alguna otra operación y deben descartarse. Por ejemplo, al llamar a consumer.seek los mensajes del lote deberían ser descartados, ya que no están en el offset al que buscamos.
            pause, // se puede utilizar para pausar el consumidor para el tema-partición actual. Todas las compensaciones resueltas hasta ese momento serán confirmadas (sujeto a eachBatchAutoResolve y autoCommit). Lanza un error para pausar en mitad del lote sin resolver el offset actual. Alternativamente, desactive eachBatchAutoResolve. La función devuelta puede utilizarse para reanudar el procesamiento del tema-partición. Consulte Pausa y reanudación para obtener más información sobre esta función
        }) => {
            for (let message of batch.messages) {
                console.log({
                    topic: batch.topic,
                    partition: batch.partition,
                    highWatermark: batch.highWatermark, // es el último desplazamiento comprometido dentro de la partición temática. Puede ser útil para calcular el desfase.
                    message: {
                        offset: message.offset,
                        key: message.key?.toString(),
                        value: message.value?.toString(),
                        headers: message?.headers,
                    }
                })
                if (!isRunning() || isStale()) break
    
                resolveOffset(message.offset) // se utiliza para marcar un mensaje del lote como procesado. En caso de error, el consumidor consignará automáticamente las compensaciones resueltas.
                
                await heartbeat() // Promise<void> se puede utilizar para enviar heartbeat al broker según el valor heartbeatInterval establecido en la configuración del consumidor, lo que significa que si invocas heartbeat() antes que heartbeatInterval será ignorado
            }
        },
    })
};

run().catch(console.error);