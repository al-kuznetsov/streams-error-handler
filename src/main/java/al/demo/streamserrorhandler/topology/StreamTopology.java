package al.demo.streamserrorhandler.topology;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import al.demo.streamserrorhandler.config.TopicsConfig;
import al.demo.streamserrorhandler.processor.ResponseSinkProcessorSupplier;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
@RequiredArgsConstructor
public class StreamTopology {
    private final TopicsConfig topicsConfig;

    @Autowired
    public void buildStreams(StreamsBuilder streamsBuilder, ResponseSinkProcessorSupplier responseSinkProcessorSupplier) {
        KStream<String, String> input = streamsBuilder.stream(topicsConfig.input());

        input
                .peek((k, v) -> log.info("Reading input record k = {}, v = {}", k, v))
                .split()
                .branch((k, v) -> v.contains("typeO"),
                        Branched.withConsumer(ks -> ks
                                .peek((k, v) -> log.info("Branched record to output topic k = {}, v = {}", k, v))
                                .to(topicsConfig.output())))
                .branch((k, v) -> v.contains("typeE"),
                        Branched.withConsumer(ks -> ks
                                .peek((k, v) -> log.info("Branched record to WRONG topic k = {}, v = {}", k, v))
                                .process(responseSinkProcessorSupplier)))
                .noDefaultBranch();

    }

}
