
package com.akiban.qp.util;

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
