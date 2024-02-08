const  kafka  = require('./config/kafkaConfig');
const logger = require('./utils/logger');

const topic = 'my-topic';
const consumer = kafka.consumer({ groupId: 'my-group' });

const run = async () => {
    // Consuming
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            logger.log({
                level: 'info',
                message: `Received message ${message.value.toString()} in partition ${partition} of topic ${topic}`,
            });

            logger.log({level:'info', message: 'start await'});
            await new Promise(resolve => setTimeout(resolve, 10000));
            logger.log({level:'info', message: 'termino await'});
        },
    });
};

run().catch(console.error);