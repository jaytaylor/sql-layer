/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
        List<Object> keyList = new ArrayList<Object>();
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
