
package com.akiban.qp.operator;

import com.akiban.qp.row.HKey;

public interface GroupCursor extends Cursor {
    void rebind(HKey hKey, boolean deep);
}
