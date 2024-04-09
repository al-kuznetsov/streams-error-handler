package al.demo.streamserrorhandler.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("topics")
public record TopicsConfig(
        String input,
        String output,
        String wrong) {
}
