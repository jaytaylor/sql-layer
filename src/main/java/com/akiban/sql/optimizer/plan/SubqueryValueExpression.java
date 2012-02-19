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

package com.akiban.sql.optimizer.plan;

import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

/** An expression evaluated by a subquery: first column of first row
 * or <code>NULL</code>.
 */
public class SubqueryValueExpression extends SubqueryExpression 
{
    public SubqueryValueExpression(Subquery subquery, 
                                   DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(subquery, sqlType, sqlSource);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SubqueryValueExpression)) return false;
        SubqueryValueExpression other = (SubqueryValueExpression)obj;
        // Currently this is ==; don't match whole subquery.
        return getSubquery().equals(other.getSubquery());
    }

    @Override
    public int hashCode() {
        return getSubquery().hashCode();
    }

}
