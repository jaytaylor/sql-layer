package com.akiban.cserver.service.tree;

import com.persistit.Exchange;

public interface TreeVisitor {

    void visit(final Exchange exchange) throws Exception;
}
