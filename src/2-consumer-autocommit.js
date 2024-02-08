const  kafka  = require('./config/kafkaConfig');
const logger = require('./utils/logger');

const topic = 'my-topic';
const consumer = kafka.consumer({ groupId: 'my-group' });

const run = async () => {
    // Consuming
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });
    await consumer.run({
        autoCommit: true,
        autoCommitInterval: 6000,
        eachMessage: async ({ topic, partition, message }) => {
            await new Promise(resolve => setTimeout(resolve, 3000));
            logger.log({
                level: 'info',
                message: `Received message ${message.value.toString()} offset ${message.offset} in partition ${partition} of topic ${topic}`,
            });
        
            if(message.value.toString() === '2') {
                throw new Error('Error en el proceso');
            }
             
        },
    });
};

run().catch(console.error);