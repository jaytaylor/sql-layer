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

import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.SchemaOBSOLETE;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.util.ArgumentValidation;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class Extract_Default extends PhysicalOperator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s)", getClass().getSimpleName(), keepTypes);
    }

    // PhysicalOperator interface

    @Override
    public List<PhysicalOperator> getInputOperators()
    {
        return Collections.singletonList(inputOperator);
    }

    @Override
    protected Cursor cursor(StoreAdapter adapter)
    {
        return new Execution(adapter, inputOperator.cursor(adapter));
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // ExtractScan_Default interface

    public Extract_Default(PhysicalOperator inputOperator, Collection<RowType> extractTypes)
    {
        ArgumentValidation.notEmpty("keepTypes", extractTypes);
        this.inputOperator = inputOperator;
        SchemaOBSOLETE schema = null;
        for (RowType type : extractTypes) {
            if (schema == null) {
                schema = type.schema();
            } else {
                ArgumentValidation.isSame("schema", schema, "type.schema()", type.schema());
            }
            if (type instanceof UserTableRowType) {
                addDescendentTypes(schema, type.userTable(), this.keepTypes);
            } else {
                this.keepTypes.add(type);
            }
        }
    }

    // For use by this class

    private static void addDescendentTypes(SchemaOBSOLETE schema, UserTable table, Set<RowType> rowTypes)
    {
        rowTypes.add(schema.userTableRowType(table));
        for (Join join : table.getChildJoins()) {
            addDescendentTypes(schema, join.getChild(), rowTypes);
        }
    }

    // Object state

    private final PhysicalOperator inputOperator;
    private final Set<RowType> keepTypes = new HashSet<RowType>();

    // Inner classes

    private class Execution implements Cursor
    {
        // Cursor interface

        @Override
        public void open(Bindings bindings)
        {
            input.open(bindings);
        }

        @Override
        public Row next()
        {
            Row row;
            do {
                row = input.next();
                if (row == null) {
                    close();
                } else if (!keepTypes.contains(row.rowType())) {
                    row = null;
                }
            } while (row == null && !closed);
            return row;
        }

        @Override
        public void close()
        {
            input.close();
            closed = true;
        }

        // Execution interface

        Execution(StoreAdapter adapter, Cursor input)
        {
            this.input = input;
        }

        // Object state

        private final Cursor input;
        private boolean closed = false;
    }
}
