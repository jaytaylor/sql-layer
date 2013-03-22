
package com.akiban.server.store;

import com.akiban.server.error.InvalidOperationException;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.exception.PersistitException;


public abstract class IndexVisitor {
    protected abstract void visit(Key key, Value value) throws PersistitException, InvalidOperationException;

    public boolean groupIndex() {
        return false;
    }

    final void visit() throws PersistitException, InvalidOperationException {
        visit(exchange.getKey(), exchange.getValue());
    }

    final void initialize(Exchange exchange)
    {
        this.exchange = exchange;
    }

    private Exchange exchange;
}
