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

package com.akiban.qp.persistitadapter;

import com.akiban.qp.row.RowBase;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.ValueSource;

final class OldRowDataCreator implements RowDataCreator<ValueSource> {

    @Override
    public ValueSource eval(RowBase row, int f) {
        return row.eval(f);
    }

    @Override
    public boolean isNull(ValueSource source) {
        return source.isNull();
    }

    @Override
    public void put(ValueSource source, NewRow into, FieldDef fieldDef, int f) {
        into.put(f, target.convertFromSource(source));
    }

    private ToObjectValueTarget target = new ToObjectValueTarget();
}
