/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.foundationdb.qp.util;

import com.persistit.Key;

public class PersistitKey
{
    public static void appendFieldFromKey(Key targetKey, Key sourceKey, int sourceDepth)
    {
        sourceKey.indexTo(sourceDepth);
        int from = sourceKey.getIndex();
        sourceKey.indexTo(sourceDepth + 1);
        int to = sourceKey.getIndex();
        if (from >= 0 && to >= 0 && to > from) {
            System.arraycopy(sourceKey.getEncodedBytes(), from,
                             targetKey.getEncodedBytes(), targetKey.getEncodedSize(), to - from);
            targetKey.setEncodedSize(targetKey.getEncodedSize() + to - from);
        }
    }
}
