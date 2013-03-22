
package com.akiban.server.error;

import com.akiban.sql.parser.QueryTreeNode;

public final class OrderGroupByNonIntegerConstant extends BaseSQLException {
    public OrderGroupByNonIntegerConstant(String which, QueryTreeNode sql) {
        super(ErrorCode.ORDER_GROUP_BY_NON_INTEGER_CONSTANT, which, sql);
    }
}
