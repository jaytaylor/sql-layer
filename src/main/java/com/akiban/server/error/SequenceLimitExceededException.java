package com.akiban.server.error;

import com.akiban.ais.model.Sequence;

public class SequenceLimitExceededException extends InvalidOperationException {
    public SequenceLimitExceededException (Sequence sequence) {
        super (ErrorCode.SEQUENCE_LIMIT_EXCEEDED, 
                sequence.getSequenceName().getSchemaName(), 
                sequence.getSequenceName().getTableName());
    }

}
