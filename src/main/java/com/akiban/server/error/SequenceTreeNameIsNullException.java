package com.akiban.server.error;

import com.akiban.ais.model.Sequence;

public class SequenceTreeNameIsNullException extends InvalidOperationException {

    public SequenceTreeNameIsNullException(Sequence sequence) {
        super(ErrorCode.SEQUENCE_TREE_NAME_NULL, 
                sequence.getSequenceName().getSchemaName(),
                sequence.getSequenceName().getTableName());
    }
}
