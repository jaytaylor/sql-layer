
package com.akiban.server.error;

import com.persistit.exception.InvalidVolumeSpecificationException;

public class InvalidVolumeException extends InvalidOperationException {
    public InvalidVolumeException (InvalidVolumeSpecificationException e) {
        super(ErrorCode.INVALID_VOLUME, e.getMessage());
    }
}
