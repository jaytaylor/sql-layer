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

package com.foundationdb.qp.persistitadapter;

import com.foundationdb.qp.row.RowBase;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.rowdata.FieldDef;
import com.foundationdb.server.types.AkType;

public interface RowDataCreator<S> {
    S eval(RowBase row, int f);
    boolean isNull(S source);
    void put(S source, NewRow into, FieldDef fieldDef, int f);
}
