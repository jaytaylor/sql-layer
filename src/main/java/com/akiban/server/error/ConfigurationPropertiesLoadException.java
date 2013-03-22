
package com.akiban.server.error;

public class ConfigurationPropertiesLoadException extends InvalidOperationException {
    public ConfigurationPropertiesLoadException (String resource, String message) {
        super(ErrorCode.CONFIG_LOAD_FAILED, resource, message);
    }
}
