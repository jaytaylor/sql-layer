
package com.akiban.server.service.plugins;

public final class PluginException extends RuntimeException {
    public PluginException(String message) {
        super(message);
    }

    public PluginException(Throwable cause) {
        super(cause);
    }
}
