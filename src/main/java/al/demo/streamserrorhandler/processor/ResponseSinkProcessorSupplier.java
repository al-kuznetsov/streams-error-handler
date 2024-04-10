package al.demo.streamserrorhandler.processor;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

@Component
public class ResponseSinkProcessorSupplier implements ProcessorSupplier<String, String, Void, Void> {
    private KafkaTemplate<String, String> kafkaTemplate;

    public ResponseSinkProcessorSupplier(KafkaProperties kafkaProperties) {
        // класс не создается Kafka Streams и Spring, сами
        Map<String, Object> configs = kafkaProperties.getStreams().buildProperties(null);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        ProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(configs);
        this.kafkaTemplate = new KafkaTemplate<>(factory);
    }

    @Override
    public Processor<String, String, Void, Void> get() {
        return new ResponseSinkProcessor(kafkaTemplate);
    }

}
