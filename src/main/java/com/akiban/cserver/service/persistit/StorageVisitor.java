package com.akiban.cserver.service.persistit;

import com.persistit.Exchange;
import com.persistit.exception.PersistitException;

public interface StorageVisitor {

    void visit(final Exchange exchange) throws Exception;
}
