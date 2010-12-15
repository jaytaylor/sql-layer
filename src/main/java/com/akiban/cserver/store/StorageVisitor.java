package com.akiban.cserver.store;

import com.persistit.Exchange;

public interface StorageVisitor {

    void visit(final Exchange exchange, Object context) throws Exception;
}
