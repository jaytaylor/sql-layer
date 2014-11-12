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

package com.foundationdb.qp.storeadapter;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.qp.expression.IndexKeyRange;

import java.util.List;

public final class SpatialHelper {
    private SpatialHelper() {

    }

    public static boolean isNullable(Index index) {
        List<IndexColumn> declaredKeys = index.getKeyColumns();
        int offset = index.firstSpatialArgument();
        for (int i = 0; i < index.spatialColumns(); i++) {
            if (declaredKeys.get(offset + i).getColumn().getNullable())
                return true;
        }
        return false;
    }

    public static boolean isNullable(IndexKeyRange indexKeyRange) {
        return isNullable(indexKeyRange.indexRowType().index());
    }
}
