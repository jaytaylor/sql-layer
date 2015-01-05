/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.store;

import com.persistit.Key;
import com.persistit.Value;

import java.util.ArrayList;
import java.util.List;

public abstract class IndexRecordVisitor extends IndexVisitor<Key,Value> {

    protected abstract void visit(List<?> key, Object value);

    @Override
    protected final void visit(Key key, Value value) {
        List<?> keyList = key(key, value);
        Object valueObj = value.isDefined() ? value.get() : null;
        visit(keyList, valueObj);
    }

    private List<?> key(Key key, Value value)
    {
        // Key traversal
        key.indexTo(0);
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
