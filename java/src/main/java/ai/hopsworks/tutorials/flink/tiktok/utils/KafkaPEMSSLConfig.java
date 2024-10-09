package ai.hopsworks.tutorials.flink.tiktok.utils;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import java.util.Properties;
import java.util.Base64;

public class KafkaPEMSSLConfig {
    public static Properties createSSLConfig(String trustStorePem, String keyStorePem, String privateKeyPem, String keyPassword) {
        Properties props = new Properties();

        // Set the security protocol to SSL
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");

        // Configure the truststore
        props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
        props.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, trustStorePem);

        // Configure the keystore
        props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM");
        props.put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, keyStorePem);
        props.put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, privateKeyPem);

        // Set the key password if provided
        if (keyPassword != null && !keyPassword.isEmpty()) {
            props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
        }

        return props;
    }

    public static String encodePEM(String pemContent) {
        // Remove PEM headers, footers, and newlines
        String cleanPem = pemContent
                .replace("-----BEGIN CERTIFICATE-----", "")
                .replace("-----END CERTIFICATE-----", "")
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replace("-----END PRIVATE KEY-----", "")
                .replaceAll("\\s", "");

        // Base64 encode the cleaned PEM content
        return Base64.getEncoder().encodeToString(cleanPem.getBytes());
    }
}
