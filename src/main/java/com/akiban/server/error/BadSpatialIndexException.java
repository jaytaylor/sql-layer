
package com.akiban.server.error;

import com.akiban.sql.parser.QueryTreeNode;

public final class BadSpatialIndexException extends BaseSQLException {
    public BadSpatialIndexException(String indexName, QueryTreeNode referenceNode) {
        super(ErrorCode.BAD_SPATIAL_INDEX, indexName, referenceNode);
    }
}
