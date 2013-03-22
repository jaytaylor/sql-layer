
package com.akiban.server.error;

public class IndistinguishableIndexException extends InvalidOperationException {
    public IndistinguishableIndexException (String indexName) {
        super (ErrorCode.INDISTINGUISHABLE_INDEX, indexName);
    }
}
