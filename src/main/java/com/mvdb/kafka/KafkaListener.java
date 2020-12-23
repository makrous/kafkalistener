package com.mvdb.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaListener {
	private static final Logger				logger		= LoggerFactory.getLogger(KafkaListener.class);
	private static Producer<String, String>	mProducer	= null;
	private static Consumer<String, String>	mConsumer	= null;

	public static void main(String[] args) {
		try {
			logger.info("Starting KafkaListener");
			new KafkaListener().execute();
		} catch (Exception e) {
			logger.error("", e);
		}
	}

	@SuppressWarnings("unused")
	private void execute() {
		try {
			logger.info("Create consumer");
			Consumer<String, String> consumer = createConsumer();
			logger.info("Subscribe to test");
			consumer.subscribe(Collections.singleton("test"));

			while (true) {
				logger.debug("Polling ...");
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				logger.debug("Polled messages: " + records.count());

				List<String> messages = new ArrayList<>();
				for (ConsumerRecord<String, String> record : records) {
					String message = record.value();
					logger.info("Message received: " + message);
					messages.add(message);
				}
				consumer.commitAsync();
				for (String message : messages) {
					this.sendMessage(message);
				}
			}
		} catch (Throwable t) {
			logger.error("", t);
		} finally {
		}
	}

	@SuppressWarnings("unused")
	private void sendMessage(String message) {
		logger.debug("Send message: " + message);
		Producer<String, String> producer = createProducer();
		logger.debug("Producer created");
		try {
			long time = System.currentTimeMillis();
			final ProducerRecord<String, String> record = new ProducerRecord<>("test2", message);
			logger.debug("Sending message to test2");
			RecordMetadata metadata = producer.send(record).get();
			long elapsedTime = System.currentTimeMillis() - time;
			System.err.printf("Sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n", record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
		} catch (Throwable t) {
			logger.error("", t);
		} finally {
			producer.flush();
		}
	}

	private static Consumer<String, String> createConsumer() {
		if (mConsumer != null) {
			return mConsumer;
		}
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.178.39:9092");
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-first-consumer-group");
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

		mConsumer = new KafkaConsumer<>(properties);
		return mConsumer;
	}

	private static Producer<String, String> createProducer() {
		if (mProducer != null) {
			return mProducer;
		}
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.178.39:9092");
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "test2");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		mProducer = new KafkaProducer<>(props);
		return mProducer;
	}
}
