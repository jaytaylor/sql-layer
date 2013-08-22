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

package com.foundationdb.server.api;

import com.foundationdb.server.error.TableDefinitionMismatchException;
import com.foundationdb.server.rowdata.RowData;

public final class LegacyUtils {
    public static Integer matchRowDatas(RowData one, RowData two) {
        if (one == null) {
            return (two == null) ? null : two.getRowDefId();
        }
        if (two == null) {
            return one.getRowDefId();
        }
        if (one.getRowDefId() == two.getRowDefId()) {
            return one.getRowDefId();
        }
        throw new TableDefinitionMismatchException (one.getRowDefId(), two.getRowDefId());
    }
}
