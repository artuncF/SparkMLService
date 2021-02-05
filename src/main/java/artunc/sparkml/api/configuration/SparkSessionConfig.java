package artunc.sparkml.api.configuration;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.io.ClassPathResource;

@Configuration
public class SparkSessionConfig {

    @Value("${app.name:SparkClassificationApp}")
    private String appName;

    @Value("${spark.bind.address:127.0.0.1}")
    private String sparkBindAddress;

    @Value("${master.uri:local[*]}")
    private String masterUri;

    @Bean
    public SparkSession sparkSession() {
        return SparkSession
                .builder()
                .appName(appName)
                .master(masterUri)
                .config("spark.driver.bindAddress",sparkBindAddress)
                .getOrCreate();
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        PropertySourcesPlaceholderConfigurer propSourceConfig = new PropertySourcesPlaceholderConfigurer();
        propSourceConfig.setLocation(new ClassPathResource("spark.properties"));
        return propSourceConfig;
    }
}
