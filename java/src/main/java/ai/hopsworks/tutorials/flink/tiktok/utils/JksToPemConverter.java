package ai.hopsworks.tutorials.flink.tiktok.utils;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Base64;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;

public class JksToPemConverter {
    public static String convertJksToPem(String jksPath, String password) throws Exception {
        // Load the JKS keystore
        KeyStore keyStore = KeyStore.getInstance("JKS");
        try (FileInputStream fis = new FileInputStream(jksPath)) {
            keyStore.load(fis, password.toCharArray());
        }

        StringBuilder pemOutput = new StringBuilder();

        List aliasesList = Collections.list(keyStore.aliases());

        System.out.println(aliasesList.size());

        for (Object aa : aliasesList) {
            String alias = aa.toString();
            // Extract the private key (if exists)
            if (keyStore.isKeyEntry(alias)) {
                PrivateKey privateKey = (PrivateKey) keyStore.getKey(alias, password.toCharArray());

                // Convert private key to PEM format
                StringWriter privateKeyWriter = new StringWriter();
                try (JcaPEMWriter pemWriter = new JcaPEMWriter(privateKeyWriter)) {
                    pemWriter.writeObject(privateKey);
                }
                pemOutput.append("Private Key:\n").append(privateKeyWriter.toString());
            }

            // Extract the certificate chain
            Certificate[] certChain = keyStore.getCertificateChain(alias);
            if (certChain != null) {
                for (Certificate cert : certChain) {
                    X509Certificate x509Cert = (X509Certificate) cert;

                    // Convert certificate to PEM format
                    StringWriter certWriter = new StringWriter();
                    try (JcaPEMWriter pemWriter = new JcaPEMWriter(certWriter)) {
                        pemWriter.writeObject(x509Cert);
                    }
                    pemOutput.append(certWriter.toString());
                }
            }
        }
        return pemOutput.toString();
    }

    public static String convertTrustStoreToPem(String trustStorePath, String password) throws Exception {
        // Load the truststore
        KeyStore trustStore = KeyStore.getInstance("JKS");
        try (FileInputStream fis = new FileInputStream(trustStorePath)) {
            trustStore.load(fis, password.toCharArray());
        }

        StringBuilder pemOutput = new StringBuilder();

        List aliasesList = Collections.list(trustStore.aliases());

        for (Object aa : aliasesList) {
            String alias = aa.toString();
            // Extract the certificate
            Certificate cert = trustStore.getCertificate(alias);
            if (cert instanceof X509Certificate) {
                X509Certificate x509Cert = (X509Certificate) cert;

                // Convert certificate to PEM format
                StringWriter certWriter = new StringWriter();
                try (JcaPEMWriter pemWriter = new JcaPEMWriter(certWriter)) {
                    pemWriter.writeObject(x509Cert);
                }
                pemOutput.append(certWriter.toString());
            }
        }
        return pemOutput.toString();
    }


    public static void main(String[] args) {
        try {
            String pemString = convertTrustStoreToPem("/Users/davitbzhalava/Desktop/certs/keyStore.jks", "PTEK7XNUWFTC8B44DU4GHA0N5HW9SWU1H4ZDUXLOESTKHP641E9FVJDJ4E608HEX");
            System.out.println(pemString);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
