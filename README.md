# My Kafka Consumer

This project is a simple Kafka consumer using Node.js and the kafkajs library.

## Prerequisites

- Node.js (v14 or later)
- Kafka running locally

## Setup

1. Clone the repository
2. Navigate to the project directory
3. Run `npm install` to install dependencies

## Running the Consumer

To start the consumer, run `node src/consumer.js` from the project directory.

The consumer is set up to consume messages from a Kafka topic. The topic and other Kafka configurations can be modified in `src/config/kafkaConfig.js`.

## Logging

The consumer logs the processing of messages. The logging utility is located in `src/utils/logger.js`.

## Contributing

Please read through our contributing guidelines. Included are directions for opening issues, coding standards, and notes on development.

## License

This project is licensed under the MIT License.