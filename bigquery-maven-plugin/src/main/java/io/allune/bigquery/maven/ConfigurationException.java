package io.allune.bigquery.maven;

public class ConfigurationException extends RuntimeException {

    public ConfigurationException(String message, Exception e) {
        super(message, e);
    }

    public ConfigurationException(String message) {
        super(message);
    }
}
