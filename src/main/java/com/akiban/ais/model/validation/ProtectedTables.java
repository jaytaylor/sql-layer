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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.message.ErrorCode;

/**
 * Verifies the only tables in the akiban_information_schema are 
 * the ones on our fixed list of tables. 
 * @author tjoneslo
 *
 */
public class ProtectedTables implements AISValidation {

    private static final Collection<String> PROTECT_LIST = createProtectList();
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (TableName tableName : ais.getUserTables().keySet()) {
            if (tableName.getSchemaName().equals(TableName.AKIBAN_INFORMATION_SCHEMA) &&
                !PROTECT_LIST.contains(tableName.getTableName())) {
                output.reportFailure(new AISValidationFailure (ErrorCode.PROTECTED_TABLE, 
                        "Can not create user table %s.%s in akiban_information_schema",
                        tableName.getSchemaName(), tableName.getTableName()));
            }
        }
        for (TableName tableName : ais.getGroupTables().keySet()) {
            if (tableName.getSchemaName().equals(TableName.AKIBAN_INFORMATION_SCHEMA) &&
                !PROTECT_LIST.contains(tableName.getTableName())) {
                output.reportFailure(new AISValidationFailure (ErrorCode.PROTECTED_TABLE,
                        "Can not create group table %s.%s in akiban_information_schema",
                        tableName.getSchemaName(), tableName.getTableName()));
            }
        }
    }
    
    /**
     * TODO: This list needs to be coordinated with the real
     * list of tables in the akiban_information_schema. The
     * list here is temporary. 
     * @return
     */
    private static Collection<String> createProtectList() {
        LinkedList<String> list = new LinkedList<String>();
        list.add("groups");
        list.add("tables");
        list.add("columns");
        list.add("joins");
        list.add("join_columns");
        list.add("indexes");
        list.add("index_columns");
        list.add("types");
        list.add("index_analysis");
        
        return Collections.unmodifiableList(list);
    }

}
