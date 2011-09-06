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

package com.akiban.sql.optimizer;

import com.akiban.sql.compiler.TypeComputer;

import com.akiban.sql.parser.*;

import com.akiban.sql.StandardException;
import com.akiban.sql.types.DataTypeDescriptor;

/** Calculate types from schema information. */
public class AISTypeComputer extends TypeComputer
{
    public AISTypeComputer() {
    }
    
    protected DataTypeDescriptor computeType(ValueNode node) throws StandardException {
        switch (node.getNodeType()) {
        case NodeTypes.COLUMN_REFERENCE:
            return columnReference((ColumnReference)node);
        default:
            return super.computeType(node);
        }
    }

    protected DataTypeDescriptor columnReference(ColumnReference node) 
            throws StandardException {
        ColumnBinding columnBinding = (ColumnBinding)node.getUserData();
        assert (columnBinding != null) : "column is not bound yet";
        return columnBinding.getType();
    }

}
