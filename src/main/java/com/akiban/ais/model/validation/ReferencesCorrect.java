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
package com.akiban.ais.model.validation;

import java.util.Map;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import com.akiban.message.ErrorCode;

public class ReferencesCorrect implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        validateTables(output, ais.getUserTables(), true);
        validateTables(output, ais.getGroupTables(), false);
    }
    
    private void validateTables (AISValidationOutput output ,
            Map<TableName, ? extends Table> tables, boolean isUserTable) {
        for (Map.Entry<TableName, ? extends Table> entry : tables.entrySet()) {
            TableName tableName = entry.getKey();
            Table table = entry.getValue();
            if (table == null) {
                output.reportFailure(new AISValidationFailure (ErrorCode.VALIDATION_FAILURE, 
                        "null table for name: %s", tableName.toString()));
            }
            if (tableName == null) {
                output.reportFailure(new AISValidationFailure (ErrorCode.VALIDATION_FAILURE,
                        "null table name detected"));
            }
            if (!tableName.equals(table.getName())) {
                output.reportFailure(new AISValidationFailure (ErrorCode.VALIDATION_FAILURE,
                        "name mismatch, expected <%s> for table <%s>",
                        tableName.toString(), table.toString()));
            }
            if(table.isGroupTable() == isUserTable) {
                output.reportFailure(new AISValidationFailure (ErrorCode.VALIDATION_FAILURE,
                        "wrong value for isGroupTable(): %s", tableName.toString()));
            }
            if (table.isUserTable() != isUserTable) {
                output.reportFailure(new AISValidationFailure (ErrorCode.VALIDATION_FAILURE,
                        "wrong value for isUserTable(): %s", tableName.toString()));
            }
            
            checkTableColumns (output, table);
            checkTableIndexes (output, table);
        }
    }
    
    private void checkTableColumns (AISValidationOutput output, Table table) {
        
        for (Column column : table.getColumnsIncludingInternal()) {
            if (column == null) {
                output.reportFailure(new AISValidationFailure (ErrorCode.VALIDATION_FAILURE,
                        "null column in table %s", table.getName().toString()));
            }
            if (column.getTable() != table) {
                output.reportFailure(new AISValidationFailure (ErrorCode.VALIDATION_FAILURE,
                        "column %s has bad reference to table %s", 
                        column.getName(), table.getName().toString()));
            }
        }
    }
    private void checkTableIndexes (AISValidationOutput output, Table table) {
        for (TableIndex index : table.getIndexes()) {
            if (index == null) {
                output.reportFailure(new AISValidationFailure (ErrorCode.VALIDATION_FAILURE, 
                        "null index in table %s", table.getName().toString()));
            }
            if (index.getTable() != table) {
                output.reportFailure(new AISValidationFailure (ErrorCode.VALIDATION_FAILURE,
                        "index %s has bad reference to table %s",
                        index.getIndexName().toString(), table.getName().toString()));
            }
            checkIndexColumns(output, index, table);
        }
    }
    
    private void checkIndexColumns (AISValidationOutput output, TableIndex index, Table table) {
        for (IndexColumn indexColumn : index.getColumns()) {
            if (!index.equals(indexColumn.getIndex())) {
                output.reportFailure(new AISValidationFailure(ErrorCode.VALIDATION_FAILURE,
                        "index column has bad reference to index %s",
                        index.getIndexName().toString()));
            }
            if (table.getColumn(indexColumn.getColumn().getName()) == null) {
                output.reportFailure(new AISValidationFailure(ErrorCode.VALIDATION_FAILURE,
                        "index %s uses column not in table %s",
                        index.getIndexName().toString(), table.getName().toString()));
            }
        }
    }
}
