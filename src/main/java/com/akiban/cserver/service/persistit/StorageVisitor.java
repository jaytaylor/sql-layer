package com.akiban.cserver.service.persistit;

import com.persistit.Exchange;

public interface StorageVisitor {

    void visit(final Exchange exchange) throws Exception;
}
