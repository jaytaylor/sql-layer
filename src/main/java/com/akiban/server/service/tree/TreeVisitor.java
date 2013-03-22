
package com.akiban.server.service.tree;

import com.persistit.Exchange;
import com.persistit.exception.PersistitException;

public interface TreeVisitor {

    void visit(final Exchange exchange) throws PersistitException;
}
