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

package com.foundationdb.ais.model.validation;

import com.foundationdb.ais.model.*;
import com.foundationdb.server.error.DuplicateConstraintNameException;
import com.foundationdb.server.error.AISNullReferenceException;
import com.foundationdb.server.error.DuplicateColumnNameException;
import com.foundationdb.server.error.DuplicateGroupNameException;
import com.foundationdb.server.error.DuplicateIndexColumnException;
import com.foundationdb.server.error.DuplicateIndexException;
import com.foundationdb.server.error.DuplicateParameterNameException;
import com.foundationdb.server.error.DuplicateRoutineNameException;
import com.foundationdb.server.error.DuplicateSequenceNameException;
import com.foundationdb.server.error.DuplicateSQLJJarNameException;
import com.foundationdb.server.error.DuplicateTableNameException;
import com.foundationdb.server.error.JoinToProtectedTableException;
import com.foundationdb.server.error.NameIsNullException;

public class AISInvariants {

    public static void checkNullField (Object field, String owner, String fieldName, String reference) {
        if (field == null) {
            throw new AISNullReferenceException(owner, fieldName, reference);
        }
    }
    
    public static void checkNullName (final String name, final String source, final String type) {
        if (name == null || name.length() == 0) {
            throw new NameIsNullException (source, type);
        }
    }
    
    public static void checkDuplicateTables(AkibanInformationSchema ais, String schemaName, String tableName)
    {
        if (ais.getColumnar(schemaName, tableName) != null) {
            throw new DuplicateTableNameException (new TableName(schemaName, tableName));
        }
    }
    
    public static void checkDuplicateSequence(AkibanInformationSchema ais, String schemaName, String sequenceName)
    {
        if (ais.getSequence(new TableName (schemaName, sequenceName)) != null) {
            throw new DuplicateSequenceNameException (new TableName(schemaName, sequenceName));
        }
    }
    
    public static void checkDuplicateColumnsInTable(Columnar table, String columnName)
    {
        if (table.getColumn(columnName) != null) {
            throw new DuplicateColumnNameException(table.getName(), columnName);
        }
    }
    public static void checkDuplicateColumnPositions(Columnar table, Integer position) {
        if (position < table.getColumns().size() &&
                table.getColumn(position) != null &&
                table.getColumn(position).getPosition().equals(position)) {
            throw new DuplicateColumnNameException (table.getName(), table.getColumn(position).getName());
        }
    }
    
    public static void checkDuplicateColumnsInIndex(Index index, TableName columnarName, String columnName)
    {
        int firstSpatialInput = Integer.MAX_VALUE;
        int lastSpatialInput = Integer.MIN_VALUE;
        if (index.isSpatial()) {
            firstSpatialInput = index.firstSpatialArgument();
            lastSpatialInput = firstSpatialInput + index.dimensions() - 1;
        }
        for(IndexColumn icol : index.getKeyColumns()) {
            int indexColumnPosition = icol.getPosition();
            if (indexColumnPosition < firstSpatialInput || indexColumnPosition > lastSpatialInput) {
                Column column = icol.getColumn();
                if (column.getName().equals(columnName) && column.getColumnar().getName().equals(columnarName)) {
                    throw new DuplicateIndexColumnException (index, columnName);
                }
            }
        }
    }
    
    public static void checkDuplicateIndexesInTable(Table table, String indexName)
    {
        if (isIndexInTable(table, indexName)) {
            throw new DuplicateIndexException (table.getName(), indexName);
        }
    }
    
    public static void checkDuplicateIndexesInGroup(Group group, String indexName)
    {
        if (group.getIndex(indexName) != null) {
            throw new DuplicateIndexException (group.getName(), indexName);
        }
    }

    public static boolean isIndexInTable (Table table, String indexName)
    {
        return table.getIndex(indexName) != null || table.getFullTextIndex(indexName) != null;
    }
 
    public static void checkDuplicateIndexColumnPosition (Index index, Integer position) {
        if (position < index.getKeyColumns().size()) {
            // TODO: Index uses position for a relative ordering, not an absolute position. 
        }
    }
    public static void checkDuplicateGroups (AkibanInformationSchema ais, TableName groupName)
    {
        if (ais.getGroup(groupName) != null) {
            throw new DuplicateGroupNameException(groupName);
        }
    }    

    public static void checkDuplicateRoutine(AkibanInformationSchema ais, String schemaName, String routineName)
    {
        if (ais.getRoutine(new TableName(schemaName, routineName)) != null) {
            throw new DuplicateRoutineNameException(new TableName(schemaName, routineName));
        }
    }
    
    public static void checkDuplicateParametersInRoutine(Routine routine, String parameterName, Parameter.Direction direction)
    {
        if (direction == Parameter.Direction.RETURN) {
            if (routine.getReturnValue() != null) {
                throw new DuplicateParameterNameException(routine.getName(), "return value");
            }
        }
        else {
            if (routine.getNamedParameter(parameterName) != null) {
                throw new DuplicateParameterNameException(routine.getName(), parameterName);
            }
        }
    }

    public static void checkDuplicateSQLJJar(AkibanInformationSchema ais, String schemaName, String jarName)
    {
        if (ais.getSQLJJar(new TableName(schemaName, jarName)) != null) {
            throw new DuplicateSQLJJarNameException(new TableName(schemaName, jarName));
        }
    }
    
    public static void checkDuplicateConstraintsInSchema(AkibanInformationSchema ais, TableName constraintName) {
        if (constraintName != null) {
            Schema schema = ais.getSchema(constraintName.getSchemaName());
            if (schema != null && schema.hasConstraint(constraintName.getTableName())) {
                throw new DuplicateConstraintNameException(constraintName);
            }
        }
    }
    
    public static void checkJoinTo(Join join, TableName childName, boolean isInternal) {
        TableName parentName = (join != null) ? join.getParent().getName() : null;
        if(parentName != null) {
            String parentSchema = parentName.getSchemaName();
            boolean inAIS = (TableName.INFORMATION_SCHEMA.equals(parentSchema) ||
                             TableName.SECURITY_SCHEMA.equals(parentSchema));
            if(inAIS && !isInternal) {
                throw new JoinToProtectedTableException(parentName, childName);
            } else if(!inAIS && isInternal) {
                throw new IllegalArgumentException("Internal table join to non-IS table: " + childName);
            }
        }
    }
    
}
