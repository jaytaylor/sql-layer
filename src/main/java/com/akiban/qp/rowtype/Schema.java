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

package com.akiban.qp.rowtype;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.Expression;

import java.util.Iterator;
import java.util.List;

public interface Schema
{
    UserTableRowType userTableRowType(UserTable table);

    IndexRowType indexRowType(Index index);

    FlattenedRowType newFlattenType(RowType parent, RowType child);

    ProjectedRowType newProjectType(List<Expression> columns);

    ProductRowType newProductType(RowType left, RowType right);

    Iterator<RowType> rowTypes();
}
