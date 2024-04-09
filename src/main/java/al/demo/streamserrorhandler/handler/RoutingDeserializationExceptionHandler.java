package al.demo.streamserrorhandler.handler;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RoutingDeserializationExceptionHandler implements DeserializationExceptionHandler {

    private KafkaTemplate<byte[], byte[]> kafkaTemplate;

    @Override
    public DeserializationHandlerResponse handle(
            ProcessorContext context,
            ConsumerRecord<byte[], byte[]> failingRecord,
            Exception exception) {
        String errorTopic = "deserialization-exception.DLT";
        byte[] key = failingRecord.key();
        byte[] value = failingRecord.value();
        try {
            log.info("Exception while deserializing producerRecord with key {} in topic {}", key,
                    failingRecord.topic());
            ProducerRecord<byte[], byte[]> resultRecord = new ProducerRecord<>(errorTopic, key, value);
            resultRecord.headers().add("error.message", exception.getMessage().getBytes(StandardCharsets.UTF_8));

            kafkaTemplate.send(resultRecord);

            log.info("Record was sent to dead letter topic {}, skipping message with key {}.", errorTopic, key);
            return DeserializationHandlerResponse.CONTINUE;
        } catch (Exception e) {
            log.error("Error while sending message with key {} to dead letter topic {}: {}", key, errorTopic,
                    e.getMessage());
            return DeserializationHandlerResponse.FAIL;
        }
    }

    @Override
    public void configure(Map<String, ?> map) {
        // this class is constructed by the kafka streams framework and not by spring,
        // hence we need to create a producer manually
        Map<String, Object> props = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, map.get(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        ProducerFactory<byte[], byte[]> factory = new DefaultKafkaProducerFactory<>(props);
        this.kafkaTemplate = new KafkaTemplate<>(factory);
    }

}
