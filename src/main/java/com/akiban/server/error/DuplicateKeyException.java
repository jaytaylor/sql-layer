
package com.akiban.server.error;

import com.persistit.Key;

public final class DuplicateKeyException extends InvalidOperationException {
    public DuplicateKeyException(String indexName, Key hKey) {
        super(ErrorCode.DUPLICATE_KEY, indexName, hKey);
    }
}
