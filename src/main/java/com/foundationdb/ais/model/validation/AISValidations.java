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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public final class AISValidations {
    public static final AISValidation CHARACTER_SET_SUPPORTED = new CharacterSetSupported();
    public static final AISValidation COLLATION_SUPPORTED = new CollationSupported();
    public static final AISValidation COLUMN_POSITION_DENSE = new ColumnPositionDense();
    public static final AISValidation GROUP_INDEX_NOT_UNIQUE = new GroupIndexesNotUnique();
    public static final AISValidation GROUP_SINGLE_ROOT = new GroupSingleRoot();
    public static final AISValidation INDEX_HAS_COLUMNS = new IndexHasColumns();
    public static final AISValidation INDEX_IDS_UNIQUE = new IndexIDsUnique();
    public static final AISValidation INDEX_SIZES = new IndexSizes();
    public static final AISValidation INDEX_TREE_NAMES_UNIQUE = new IndexTreeNamesUnique();
    public static final AISValidation JOIN_COLUMN_TYPES_MATCH = new JoinColumnTypesMatch();
    public static final AISValidation JOIN_TO_PARENT_PK = new JoinToParentPK();
    public static final AISValidation JOIN_TO_ONE_PARENT = new JoinToOneParent();
    public static final AISValidation PRIMARY_KEY_IS_NOT_NULL = new PrimaryKeyIsNotNull();
    public static final AISValidation REFERENCES_CORRECT = new ReferencesCorrect();
    public static final AISValidation SUPPORTED_COLUMN_TYPES = new SupportedColumnTypes();    
    public static final AISValidation TABLE_HAS_PRIMARY_KEY = new TableHasPrimaryKey();
    public static final AISValidation TABLEID_UNIQUE = new TableIDsUnique();
    public static final AISValidation TABLES_IN_A_GROUP = new TablesInAGroup();
    public static final AISValidation GROUP_TREE_NAMES_UNIQUE = new GroupTreeNamesUnique();
    public static final AISValidation TYPES_ARE_FROM_STATIC = new TypesAreFromStatic();
    public static final AISValidation GROUP_INDEX_DEPTH = new GroupIndexDepth();
    public static final AISValidation TREE_NAMES_NOT_NULL = new TreeNamesAreNotNull();
    public static final AISValidation MEMORY_TABLES_NOT_MIXED = new MemoryTablesNotMixed();
    public static final AISValidation MEMORY_TABLES_SINGLE = new MemoryTableSingleTableGroup();
    public static final AISValidation VIEW_REFERENCES = new ViewReferences();
    public static final AISValidation INDEX_COLUMN_IS_NOT_PARTIAL = new IndexColumnIsNotPartial();
    public static final AISValidation COLUMN_SIZES_MATCH = new ColumnMaxAndPrefixSizesMatch();
    public static final AISValidation SEQUENCE_VALUES_VALID = new SequenceValuesValid();
    public static final AISValidation INDEX_IDS_POSITIVE = new IndexIDsPositive();

    public static final Collection<AISValidation> LIVE_AIS_VALIDATIONS = buildValidationList();
    
    private static Collection<AISValidation> buildValidationList() {
        Collection<AISValidation> validations = Collections.unmodifiableList(Arrays.asList(
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
                GROUP_SINGLE_ROOT,
                GROUP_INDEX_NOT_UNIQUE,
                JOIN_TO_ONE_PARENT,
                JOIN_TO_PARENT_PK,
                JOIN_COLUMN_TYPES_MATCH,
                GROUP_TREE_NAMES_UNIQUE,
                INDEX_TREE_NAMES_UNIQUE,
                TYPES_ARE_FROM_STATIC,
                GROUP_INDEX_DEPTH,
                TREE_NAMES_NOT_NULL,
                MEMORY_TABLES_NOT_MIXED,
                MEMORY_TABLES_SINGLE,
                //VIEW_REFERENCES
                //CHARACTER_SET_SUPPORTED
                COLLATION_SUPPORTED,
                INDEX_COLUMN_IS_NOT_PARTIAL,
                COLUMN_SIZES_MATCH,
                SEQUENCE_VALUES_VALID,
                INDEX_IDS_POSITIVE
                ));

        // Since we have one instance of the validation that is reused, they may not contain instance data.
        // Added to prevent something like bug1078746 happening again (instance data changing concurrently).
        for(AISValidation v : validations) {
            for(Field f : v.getClass().getDeclaredFields()) {
                if((f.getModifiers() & Modifier.STATIC) == 0) {
                    throw new IllegalStateException("Field " + f.getName() + " of " + v.getClass().getName() + " is not static");
                }
            }
        }

        return validations;
    }
    
    private AISValidations () {}
}
