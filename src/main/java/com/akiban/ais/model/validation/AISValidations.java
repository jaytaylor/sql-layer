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

import java.util.ArrayList;
import java.util.Collection;

public final class AISValidations {
    //public static final AISValidation NO_NULL_IDENTIFIERS;
    public static final AISValidation GROUP_TABLE_SINGLE_ROOT = new GroupTableSingleRoot();
    public static final AISValidation PROTECTED_TABLES = new ProtectedTables();
    public static final AISValidation REFERENCES_CORRECT = new ReferencesCorrect();
    public static final AISValidation SUPPORTED_COLUMN_TYPES = new SupportedColumnTypes();    
    public static final AISValidation TABLE_COLUMNS_MATCH_GROUP = new TableColumnsMatchGroupColumns();
    public static final AISValidation TABLEID_UNIQUE = new TableIDsUnique();
    public static final AISValidation TABLE_INDEXES_MATCH_GROUP = new TableIndexesMatchGroupIndexes();
    public static final AISValidation TABLES_IN_A_GROUP = new TablesInAGroup();
    
    public static final Collection<AISValidation> ALL_VALIDATIONS;
    
    static {
        ALL_VALIDATIONS = new ArrayList<AISValidation> (10);
        ALL_VALIDATIONS.add(PROTECTED_TABLES);
        ALL_VALIDATIONS.add(SUPPORTED_COLUMN_TYPES);
        ALL_VALIDATIONS.add(TABLEID_UNIQUE);
        ALL_VALIDATIONS.add(REFERENCES_CORRECT);
        ALL_VALIDATIONS.add(TABLE_COLUMNS_MATCH_GROUP);
        ALL_VALIDATIONS.add(TABLE_INDEXES_MATCH_GROUP);
        ALL_VALIDATIONS.add(TABLES_IN_A_GROUP);
        ALL_VALIDATIONS.add(GROUP_TABLE_SINGLE_ROOT);
    }
    
    private AISValidations () {}
}
