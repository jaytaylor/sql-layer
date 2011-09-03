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
package com.akiban.qp.physicaloperator;

import java.util.List;

import com.akiban.qp.expression.Expression;
import com.akiban.qp.rowtype.ProjectedRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.util.ArgumentValidation;
/**
 * Project_Table performs a similar function to the Project_Default operator, except
 * the resulting projection is onto a specific user table RowType. 
 * 
 * Insert_Default, Update_Default, Delete_Default use Project_Default to project their select 
 * results into the UserTableRowType for the table being modified
 * 
 * @author tjoneslo
 *
 */
class Project_Table extends Project_Default {

    public Project_Table(PhysicalOperator inputOperator, RowType rowType, List<Expression> projections)
    {
        super (inputOperator, rowType, projections);
        ArgumentValidation.isTrue("RowType has UserTable", rowType.hasUserTable());
        projectType = new ProjectedRowType(rowType.schema(), rowType.typeId(), projections); 
    }

    @Override
    public String toString()
    {
        return String.format("project to table %s (%s)", rowType.userTable(), projections);
    }


}
