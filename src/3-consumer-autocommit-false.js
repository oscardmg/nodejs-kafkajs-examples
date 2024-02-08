const  kafka  = require('./config/kafkaConfig');
const logger = require('./utils/logger');

const topic = 'my-topic';
const consumer = kafka.consumer({ groupId: 'my-group' });

const run = async () => {
    // Consuming
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });
    let cont = 0;
    await consumer.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message }) => {
            const offset = parseInt(message.offset) + 1;
            await new Promise(resolve => setTimeout(resolve, 200));
            logger.log({level: 'info',message: `Received message ${message.value.toString()} offset ${message.offset} in partition ${partition} of topic ${topic}`});

            // throw new Error('Error en el proceso');
            // cont++;
            if(cont === 5) {
                await consumer.commitOffsets([
                    { topic, partition, offset: offset },
                ]);
                logger.log({
                    level: 'info',
                    message: `Confirmar offset ${offset}`,
                });
                cont = 0;
            }
        },
    });
};

run().catch(console.error);