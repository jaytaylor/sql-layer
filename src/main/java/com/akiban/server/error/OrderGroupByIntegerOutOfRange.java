
package com.akiban.server.error;

public final class OrderGroupByIntegerOutOfRange extends InvalidOperationException {
    public OrderGroupByIntegerOutOfRange(String which, int index, int limit) {
        super(ErrorCode.ORDER_GROUP_BY_INTEGER_OUT_OF_RANGE, which, index, limit);
    }
}
