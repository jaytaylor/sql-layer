
package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.TableIndex;
import com.akiban.qp.expression.IndexKeyRange;

import java.util.List;

public final class SpatialHelper {
    private SpatialHelper() {

    }

    public static boolean isNullable(Index index) {
        List<IndexColumn> declaredKeys = index.getKeyColumns();
        int offset = index.firstSpatialArgument();
        for (int i = 0; i < index.dimensions(); i++) {
            if (declaredKeys.get(offset + i).getColumn().getNullable())
                return true;
        }
        return false;
    }

    public static boolean isNullable(IndexKeyRange indexKeyRange) {
        return isNullable(indexKeyRange.indexRowType().index());
    }
}
