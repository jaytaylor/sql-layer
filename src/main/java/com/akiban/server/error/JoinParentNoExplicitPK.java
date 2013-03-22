package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class JoinParentNoExplicitPK extends InvalidOperationException {
    public JoinParentNoExplicitPK (TableName parent) {
        super (ErrorCode.JOIN_PARENT_NO_PK, parent.getSchemaName(), parent.getTableName());
    } 
}
