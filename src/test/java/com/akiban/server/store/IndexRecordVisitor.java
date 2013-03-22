
package com.akiban.server.store;

import com.akiban.server.error.InvalidOperationException;
import com.akiban.util.Undef;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.exception.PersistitException;

import java.util.ArrayList;
import java.util.List;

public abstract class IndexRecordVisitor extends IndexVisitor {

    protected abstract void visit(List<?> key, Object value);

    @Override
    protected final void visit(Key key, Value value) throws PersistitException, InvalidOperationException {
        List<?> keyList = key(key, value);
        Object valueObj = value.isDefined() ? value.get() : Undef.only();
        visit(keyList, valueObj);
    }

    private List<?> key(Key key, Value value)
    {
        // Key traversal
        List<Object> keyList = new ArrayList<>();
        extractKeySegments(key, keyList);
        // Value traversal. If the value is defined, then it contains more fields encoded like a key.
        // TODO: What about group indexes?
        if (!groupIndex() && value.isDefined()) {
            Key buffer = new Key(key);
            buffer.clear();
            value.getByteArray(buffer.getEncodedBytes(), 0, 0, value.getArrayLength());
            buffer.setEncodedSize(value.getArrayLength());
            extractKeySegments(buffer, keyList);
        }
        return keyList;
    }

    private void extractKeySegments(Key key, List<Object> list)
    {
        int segments = key.getDepth();
        for (int s = 0; s < segments; s++) {
            list.add(key.decode());
        }
    }
}
