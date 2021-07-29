package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {

    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFunction parse;


    public KafkaService(ConsumerFunction parse, String classSimpleNome, Class<T> type, Map<String, String> properties) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(type, classSimpleNome, properties));
    }

    public KafkaService(String classSimpleNome, String topic, ConsumerFunction parse, Class<T> type, Map<String, String> properties) {
        this(parse, classSimpleNome, type, properties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(String classSimpleNome, Pattern topic, ConsumerFunction parse, Class<T> type, Map<String, String> properties) {
        this(parse, classSimpleNome, type, properties);
        consumer.subscribe(topic);
    }

    public void run() {
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));

            if (!records.isEmpty()) {
                System.out.println("encontrei registros " + records.count() + " Registros");

                for (var record : records) {
                    parse.consume(record);
                }
            }

        }
    }

    private Properties getProperties(Class<T> type, String classSimpleNome, Map<String, String> overrideProperties) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, classSimpleNome);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
        properties.putAll(overrideProperties);
        return properties;
    }


    @Override
    public void close() throws IOException {
        consumer.close();
    }
}
