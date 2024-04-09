package al.demo.streamserrorhandler.processor;

import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ResponseSinkProcessor implements Processor<String, String, Void, Void> {

    private ProcessorContext<Void, Void> context;
    private KafkaTemplate<String, String> kafkaTemplate;

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        this.context = context;
        // класс не создается Kafka Streams и Spring, сами
        Map<String, Object> props = Map.of(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093",
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT",
                SaslConfigs.SASL_MECHANISM, "PLAIN",
                SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"nana\" password=\"nana-secret\";",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        ProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(props);
        this.kafkaTemplate = new KafkaTemplate<>(factory);

    }

    @Override
    public void process(org.apache.kafka.streams.processor.api.Record<String, String> routedRecord) {
        log.info("Start processing in Error Handler");
        String targetTopic = "demo_output";
        String key = routedRecord.key();
        String value = routedRecord.value();

        ProducerRecord<String, String> resultRecord = new ProducerRecord<>(targetTopic, key, value);
        try {
            log.info("Exception while sending producerRecord with key {}", key);
            // resultRecord.headers().add("error.message", "routed manually".getBytes(StandardCharsets.UTF_8));

            kafkaTemplate.send(resultRecord);

            log.info("Record was sent to target topic {}.", targetTopic);
        } catch (Exception e) {
            log.error("Error while sending message with key {} to dead letter topic {}: {}", key, targetTopic,
                    e.getMessage());
            kafkaTemplate.send(resultRecord);
        }

    }
}
