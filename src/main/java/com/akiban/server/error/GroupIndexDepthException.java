
package com.akiban.server.error;

import com.akiban.ais.model.GroupIndex;

public final class GroupIndexDepthException extends InvalidOperationException {
    public GroupIndexDepthException(GroupIndex index, int depth) {
        super(ErrorCode.GROUP_INDEX_DEPTH, index.getIndexName(), depth);
    }
}
