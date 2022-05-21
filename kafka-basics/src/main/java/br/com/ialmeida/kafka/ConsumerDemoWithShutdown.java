package br.com.ialmeida.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());
    public static final String HOST = "127.0.0.1:9092";

    public static void main(String[] args) {
        LOG.info("I am a Kafka Consumer!");

        String TOPIC = "demo_java";
        String GROUP_ID = "my-third-application";

        // create Consumer Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  // none/earliest/latest

        // create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                LOG.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // subscribe Consumer to our Topic(s)
            consumer.subscribe(Collections.singletonList(TOPIC));

            // poll for new data
            while (true) {
                LOG.info("Polling...");

                // waiting for records for a maximum of 1000 ms
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    LOG.info(
                            "Key: " + record.key() + "\n" +
                                    "Value: " + record.value() + "\n" +
                                    "Partition: " + record.partition() + "\n" +
                                    "Offset: " + record.offset() + "\n"
                    );
                }
            }
        } catch (WakeupException e) {
            LOG.info("Wake up exception!");
            // we ignore this as this is an expected exception when closing a Consumer
        } catch (Exception e) {
            LOG.error("Unexpected exception!");
        } finally {
            consumer.close();  // this will also commit the offsets if need be
            LOG.info("The Consumer is now gracefully closed!");
        }

    }

}
