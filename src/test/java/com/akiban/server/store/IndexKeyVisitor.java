
package com.akiban.server.store;

import java.util.*;

public abstract class IndexKeyVisitor extends IndexRecordVisitor
{
    protected abstract void visit(List<?> key);

    @Override
    protected final void visit(List<?> key, Object value) {
        visit(key);
    }
}
