
package com.akiban.server.error;

public class BadConfigDirectoryException extends InvalidOperationException {
    public BadConfigDirectoryException(String configDirectory) {
        super (ErrorCode.BAD_CONFIG_DIRECTORY, configDirectory);
    }
}
