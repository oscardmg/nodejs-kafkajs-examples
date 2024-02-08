const kafka = require('./config/kafkaConfig');

const admin = kafka.admin();

const run = async () => {
  await admin.connect();

  console.log('topics: ', await admin.listTopics());
  console.log('groups: ', await admin.listGroups());
  console.log('topic metadata: ', await admin.fetchTopicMetadata());
  console.log('offsets: ', await admin.fetchTopicOffsets('my-topic'));

  console.log('describe groups: ', await admin.describeGroups(['my-group']));
  
  // await admin.resetOffsets({ 'my-group', 'my-topic' }) ;
  
  
  await admin.disconnect();
  
  
  
};

run().catch((e) => console.error(e));
