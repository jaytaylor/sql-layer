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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public final class AISValidations {
    //public static final AISValidation NO_NULL_IDENTIFIERS;
    public static final AISValidation CHARACTER_SET_SUPPORTED = new CharacterSetSupported();
    public static final AISValidation COLUMN_POSITION_DENSE = new ColumnPositionDense();
    public static final AISValidation GROUP_INDEX_NOT_UNIQUE = new GroupIndexesNotUnique();
    public static final AISValidation GROUP_TABLE_SINGLE_ROOT = new GroupTableSingleRoot();
    public static final AISValidation INDEX_HAS_COLUMNS = new IndexHasColumns();
    public static final AISValidation INDEX_IDS_UNIQUE = new IndexIDsUnique();
    public static final AISValidation INDEX_SIZES = new IndexSizes();
    public static final AISValidation INDEX_TREE_NAMES_UNIQUE = new IndexTreeNamesUnique();
    public static final AISValidation JOIN_COLUMN_TYPES_MATCH = new JoinColumnTypesMatch();
    public static final AISValidation JOIN_TO_PARENT_PK = new JoinToParentPK();
    public static final AISValidation JOIN_TO_ONE_PARENT = new JoinToOneParent();
    public static final AISValidation PRIMARY_KEY_IS_NOT_NULL = new PrimaryKeyIsNotNull();
    public static final AISValidation PROTECTED_TABLES = new ProtectedTables();
    public static final AISValidation REFERENCES_CORRECT = new ReferencesCorrect();
    public static final AISValidation SUPPORTED_COLUMN_TYPES = new SupportedColumnTypes();    
    public static final AISValidation TABLE_COLUMNS_MATCH_GROUP = new TableColumnsMatchGroupColumns();
    public static final AISValidation TABLE_INDEXES_MATCH_GROUP = new TableIndexesMatchGroupIndexes();
    public static final AISValidation TABLE_HAS_PRIMARY_KEY = new TableHasPrimaryKey();
    public static final AISValidation TABLEID_UNIQUE = new TableIDsUnique();
    public static final AISValidation TABLES_IN_A_GROUP = new TablesInAGroup();
    public static final AISValidation TABLES_IN_GROUP_SAME_TREE_NAME = new TablesInGroupSameTreeName();
    public static final AISValidation TABLE_TREE_NAMES_UNIQUE = new TableTreeNamesUnique();
    public static final AISValidation TYPES_ARE_FROM_STATIC = new TypesAreFromStatic();
    public static final AISValidation GROUP_INDEX_DEPTH = new GroupIndexDepth();
    
    public static final Collection<AISValidation> LIVE_AIS_VALIDATIONS;
    
    static {
        LIVE_AIS_VALIDATIONS = Collections.unmodifiableList(Arrays.asList(
                PROTECTED_TABLES,
                TABLE_HAS_PRIMARY_KEY,
                PRIMARY_KEY_IS_NOT_NULL,
                SUPPORTED_COLUMN_TYPES,
                COLUMN_POSITION_DENSE,
                TABLEID_UNIQUE,
                INDEX_IDS_UNIQUE,
                REFERENCES_CORRECT,
                TABLES_IN_A_GROUP,
                INDEX_HAS_COLUMNS,
                INDEX_SIZES,
                TABLE_COLUMNS_MATCH_GROUP,
                TABLE_INDEXES_MATCH_GROUP,
                GROUP_TABLE_SINGLE_ROOT,
                GROUP_INDEX_NOT_UNIQUE,
                JOIN_TO_ONE_PARENT,
                JOIN_TO_PARENT_PK,
                JOIN_COLUMN_TYPES_MATCH,
                TABLES_IN_GROUP_SAME_TREE_NAME,
                TABLE_TREE_NAMES_UNIQUE,
                INDEX_TREE_NAMES_UNIQUE,
                TYPES_ARE_FROM_STATIC,
                GROUP_INDEX_DEPTH
                //CHARACTER_SET_SUPPORTED
                ));
    }
    
    private AISValidations () {}
}
