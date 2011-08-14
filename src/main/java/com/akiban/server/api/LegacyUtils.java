/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.api;

import com.akiban.server.rowdata.RowData;
import com.akiban.server.api.dml.TableDefinitionMismatchException;

public final class LegacyUtils {
    public static Integer matchRowDatas(RowData one, RowData two) throws TableDefinitionMismatchException {
        if (one == null) {
            return (two == null) ? null : two.getRowDefId();
        }
        if (two == null) {
            return one.getRowDefId();
        }
        if (one.getRowDefId() == two.getRowDefId()) {
            return one.getRowDefId();
        }
        throw new TableDefinitionMismatchException(String.format("Mismatched table ids: %d != %d",
                one.getRowDefId(), two.getRowDefId()));
    }
}
