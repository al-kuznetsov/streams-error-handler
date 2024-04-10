package al.demo.streamserrorhandler.processor;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.springframework.kafka.core.KafkaTemplate;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ResponseSinkProcessor implements Processor<String, String, Void, Void> {

    private ProcessorContext<Void, Void> context;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        this.context = context;
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

            kafkaTemplate.send(resultRecord);

            log.info("Record was sent to target topic {}.", targetTopic);
        } catch (Exception e) {
            log.error("Error while sending message with key {} to dead letter topic {}: {}", key, targetTopic,
                    e.getMessage());
            kafkaTemplate.send(resultRecord);
        }

    }
}
