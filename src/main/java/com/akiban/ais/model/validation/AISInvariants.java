
package com.akiban.ais.model.validation;

import com.akiban.ais.model.*;
import com.akiban.server.error.AISNullReferenceException;
import com.akiban.server.error.DuplicateColumnNameException;
import com.akiban.server.error.DuplicateGroupNameException;
import com.akiban.server.error.DuplicateIndexColumnException;
import com.akiban.server.error.DuplicateIndexException;
import com.akiban.server.error.DuplicateParameterNameException;
import com.akiban.server.error.DuplicateRoutineNameException;
import com.akiban.server.error.DuplicateSequenceNameException;
import com.akiban.server.error.DuplicateSQLJJarNameException;
import com.akiban.server.error.DuplicateTableNameException;
import com.akiban.server.error.NameIsNullException;

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
        if (position < table.getColumnsIncludingInternal().size() && 
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
        return table.getIndex(indexName) != null;
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
}
