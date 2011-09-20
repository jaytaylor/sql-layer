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

package com.akiban.server.error;

import java.util.ResourceBundle;

/**
 * The error codes that are part of the AkSserver's public API.
 *
 * <p>Each enum below has a unique, short-typed value. That value consists of two components, a group and a subcode.
 * The group is a number 0 &lt;= G &lt;= 31, and the subcode is a number 0 &lt;= N &lt;= 999. They are combined to create
 * a number <tt>GGNNN</tt>, where the <tt>NNN</tt> is 0-padded.</p>
 *
 * <p>Groups are organized as such:
 * <ul>
 *  <li><b>0</b>: Generic errors, when we don't know what went wrong</li>
 *  <li><b>1-19</b>: User errors; the user tried to do something that's not allowed</li>
 *  <li><b>21-29</b>: Client errors; the client broke the API</li>
 *  <li><b>30</b>: Server errors, like if a transaction couldn't be committed</li>
 *  <li><b>31</b>: Bad server errors, like data corruption.</li>
 * </ul>
 * </p>
 *
 * <p>Within a group, "real" errors &mdash; those we expect to be there forever &mdash; count up from 0, and
 * "temporarily unsupported" errors &mdash; when we explicitly catch something that we don't support but plan to
 * eventually &mdash; count up from 900.</p>
 *
 * <p>These conventions let users quickly identify the nature of the error by looking at the group and the hundreds
 * digit of the subcode.</p>
 */
public enum ErrorCode {
    // Generic codes
    UNKNOWN                 (0, 0, Importance.ERROR, null),
    UNEXPECTED_EXCEPTION    (0, 1, Importance.ERROR, null),
    UNSUPPORTED_OPERATION   (0, 900, Importance.ERROR, null),

    // AkSserver and Head are out of sync
    SERVER_SHUTDOWN         (1, 0, Importance.DEBUG, null),
    STALE_AIS               (1, 1, Importance.TRACE, OldAISException.class),
    METAMODEL_MISMATCH      (1, 2, Importance.ERROR, MetaModelVersionMismatchException.class),

    // DDL errors
    PARSE_EXCEPTION         (2, 0, Importance.DEBUG, ParseException.class),
    UNSUPPORTED_CHARSET     (2, 1, Importance.DEBUG, UnsupportedCharsetException.class), 
    PROTECTED_TABLE         (2, 2, Importance.DEBUG, ProtectedTableDDLException.class), 
    JOIN_TO_PROTECTED_TABLE (2, 3, Importance.DEBUG, JoinToProtectedTableException.class), 
    JOIN_TO_UNKNOWN_TABLE   (2, 4, Importance.DEBUG, JoinToUnknownTableException.class),  
    JOIN_TO_WRONG_COLUMNS   (2, 5, Importance.DEBUG, JoinToWrongColumnsException.class), 
    DUPLICATE_TABLE         (2, 6, Importance.DEBUG, DuplicateTableNameException.class), 
    UNSUPPORTED_DROP        (2, 7, Importance.DEBUG, UnsupportedDropException.class),
    UNSUPPORTED_DATA_TYPE   (2, 8, Importance.DEBUG, UnsupportedDataTypeException.class),
    JOIN_TO_MULTIPLE_PARENTS(2, 9, Importance.DEBUG, JoinToMultipleParentsException.class), 
    UNSUPPORTED_INDEX_DATA_TYPE(2, 10, Importance.DEBUG, UnsupportedIndexDataTypeException.class),
    UNSUPPORTED_INDEX_SIZE  (2, 11, Importance.DEBUG, UnsupportedIndexSizeException.class),
    DUPLICATE_COLUMN        (2, 12, Importance.DEBUG, DuplicateColumnNameException.class),
    DUPLICATE_GROUP         (2, 13, Importance.DEBUG, DuplicateGroupNameException.class), 
    REFERENCED_TABLE        (2, 14, Importance.DEBUG, ReferencedTableException.class),  
    DROP_INDEX_NOT_ALLOWED  (2, 15, Importance.DEBUG, DropIndexNotAllowedException.class), //NOT USED
    FK_DDL_VIOLATION        (2, 16, Importance.DEBUG, ForeignConstraintDDLException.class),
    PROTECTED_INDEX         (2, 17, Importance.DEBUG, ProtectedIndexException.class),
    BRANCHING_GROUP_INDEX   (2, 18, Importance.DEBUG, BranchingGroupIndexException.class),
    WRONG_NAME_FORMAT       (2, 19, Importance.DEBUG, WrongNameFormatException.class),
    DUPLICATE_VIEW          (2, 20, Importance.DEBUG, DuplicateViewException.class),
    UNDEFINED_VIEW          (2, 21, Importance.DEBUG, UndefinedViewException.class),
    SUBQUERY_ONE_COLUMN     (2, 22, Importance.DEBUG, SubqueryOneColumnException.class),
    DUPLICATE_SCHEMA        (2, 23, Importance.DEBUG, DuplicateSchemaException.class),
    DROP_SCHEMA_NOT_ALLOWED (2, 24, Importance.DEBUG, DropSchemaNotAllowedException.class),
    WRONG_TABLE_FOR_INDEX   (2, 25, Importance.DEBUG, WrongTableForIndexException.class),
    MISSING_DDL_PARAMETERS  (2, 26, Importance.DEBUG, MissingDDLParametersException.class),
    INDEX_COL_NOT_IN_GROUP  (2, 27, Importance.DEBUG, IndexColNotInGroupException.class),
    INDEX_TABLE_NOT_IN_GROUP(2, 28, Importance.DEBUG, IndexTableNotInGroupException.class),
    
    // DML errors
    NO_REFERENCED_ROW       (3, 0, Importance.DEBUG, null),
    DUPLICATE_KEY           (3, 1, Importance.DEBUG, DuplicateKeyException.class),
    NO_SUCH_TABLE           (3, 2, Importance.DEBUG, NoSuchTableException.class), 

    NO_INDEX                (3, 4, Importance.DEBUG, NoSuchIndexException.class),
    NO_SUCH_RECORD          (3, 5, Importance.DEBUG, null),
    FK_CONSTRAINT_VIOLATION (3, 6, Importance.DEBUG, ForeignKeyConstraintDMLException.class),
    UNSUPPORTED_MODIFICATION(3, 7, Importance.DEBUG, null),

    TABLEDEF_MISMATCH       (3, 9, Importance.DEBUG, TableDefinitionMismatchException.class), 
    NO_SUCH_ROW             (3, 10, Importance.DEBUG,  NoSuchRowException.class),
    CONCURRENT_MODIFICATION (3, 11, Importance.DEBUG, ConcurrentScanAndUpdateException.class), 
    TABLE_DEFINITION_CHANGED(3, 12, Importance.DEBUG, TableDefinitionChangedException.class),
    NO_SUCH_GROUP           (3, 13, Importance.DEBUG, NoSuchGroupException.class), 
    NO_SUCH_TABLEDEF        (3, 14, Importance.DEBUG, RowDefNotFoundException.class), 
    NO_ROWS_UPDATED         (3, 15, Importance.DEBUG, NoRowsUpdatedException.class),    
    TOO_MANY_ROWS_UPDATED   (3, 16, Importance.DEBUG, TooManyRowsUpdatedException.class),  
    NO_SUCH_TABLEID         (3, 17, Importance.DEBUG, NoSuchTableIdException.class),
    SCAN_RETRY_ABANDONDED   (3, 18, Importance.ERROR, ScanRetryAbandonedException.class),
    NO_TRANSACTION          (3, 19, Importance.DEBUG, NoTransactionInProgressException.class),
    TRANSACTION_IN_PROGRESS (3, 20, Importance.DEBUG, TransactionInProgressException.class),
    SELECT_EXISTS_ERROR     (3, 21, Importance.DEBUG, SelectExistsErrorException.class),
    AMBIGUOUS_COLUMN_NAME   (3, 22, Importance.DEBUG, AmbiguousColumNameException.class),
    UNABLE_TO_EXPLAIN       (3, 23, Importance.DEBUG, UnableToExplainException.class),
    SUBQUERY_RESULT_FAIL    (3, 24, Importance.DEBUG, SubqueryResultsSetupException.class),
    JOIN_NODE_ERROR         (3, 25, Importance.DEBUG, JoinNodeAdditionException.class),
    MULTIPLE_JOINS          (3, 26, Importance.DEBUG, MultipleJoinsToTableException.class),
    VIEW_BAD_SUBQUERY       (3, 27, Importance.DEBUG, ViewHasBadSubqueryException.class),
    TABLE_BAD_SUBQUERY      (3, 28, Importance.DEBUG, TableIsBadSubqueryException.class),
    WRONG_FUNCTION_ARITY    (3, 29, Importance.DEBUG, WrongExpressionArityException.class),
    NO_SUCH_FUNCTION        (3, 31, Importance.DEBUG, NoSuchFunctionException.class),
    ORDER_BY_NON_INTEGER_CONSTANT(3, 32, Importance.DEBUG, OrderByNonIntegerConstant.class),
    ORDER_BY_INTEGER_OUT_OF_RANGE(3, 34, Importance.DEBUG, OrderByIntegerOutOfRange.class),
    
    ROW_OUTPUT              (4, 11, Importance.DEBUG, RowOutputException.class), 
    AIS_MYSQL_SQL_EXCEPTION (4, 12, Importance.DEBUG, AisSQLErrorException.class),
    AIS_CSV_ERROR           (4, 13, Importance.DEBUG, AisCSVErrorException.class),

    INSERT_NULL_CHECK       (5, 01, Importance.DEBUG, InsertNullCheckFailedException.class),
    
    // Messaging errors
    MALFORMED_REQUEST       (21, 0, Importance.ERROR, null), 
    BAD_STATISTICS_TYPE     (21, 4, Importance.ERROR, BadStatisticsTypeException.class),

    // AIS Validation errors, Attempts to modify and build an AIS failed
    // due to missing or invalid information.
    VALIDATION_FAILURE      (22, 0, Importance.DEBUG, null),
    INTERNAL_REFERENCES_BROKEN(22, 1, Importance.DEBUG, null),
    GROUP_MULTIPLE_ROOTS (22,  2, Importance.DEBUG, GroupHasMultipleRootsException.class),
    JOIN_TYPE_MISMATCH   (22,  3, Importance.DEBUG, JoinColumnTypesMismatchException.class),
    PK_NULL_COLUMN       (22,  4, Importance.DEBUG, PrimaryKeyNullColumnException.class),
    DUPLICATE_INDEXES    (22,  5, Importance.DEBUG, DuplicateIndexException.class),
    MISSING_PRIMARY_KEY  (22,  6, Importance.DEBUG, NoPrimaryKeyException.class),
    DUPLICATE_TABLEID    (22,  7, Importance.DEBUG, DuplicateTableIdException.class),
    JOIN_COLUMN_MISMATCH (22,  8, Importance.DEBUG, JoinColumnMismatchException.class),
    INDEX_LACKS_COLUMNS  (22,  9, Importance.DEBUG, IndexLacksColumnsException.class),
    NO_SUCH_COLUMN       (22, 10, Importance.DEBUG, NoSuchColumnException.class),
    DUPLICATE_INDEX_TREENAME (22, 11, Importance.DEBUG, DuplicateIndexTreeNamesException.class),
    DUPLICATE_TABLE_TREENAME (22, 12, Importance.DEBUG, DuplicateTableTreeNamesException.class),
    TABLE_NOT_IN_GROUP   (22, 13, Importance.DEBUG, TableNotInGroupException.class),
    NAME_IS_NULL         (22, 14, Importance.DEBUG, NameIsNullException.class),
    DUPLICATE_INDEX_COLUMN (22, 15, Importance.DEBUG, DuplicateIndexColumnException.class),
    COLUMN_POS_ORDERED   (22, 16, Importance.DEBUG, ColumnPositionNotOrderedException.class),
    TABLE_COL_IN_GROUP   (22, 17, Importance.DEBUG, TableColumnNotInGroupException.class),
    GROUP_MISSING_COL    (22, 18, Importance.DEBUG, GroupMissingTableColumnException.class),
    GROUP_MISSING_INDEX  (22, 19, Importance.DEBUG, GroupMissingIndexException.class),
    TREENAME_MISMATCH    (22, 20, Importance.DEBUG, TreeNameMismatchException.class),
    NULL_REFERENCE       (22, 21, Importance.DEBUG, AISNullReferenceException.class),
    BAD_AIS_REFERENCE    (22, 22, Importance.DEBUG, BadAISReferenceException.class),
    BAD_INTERNAL_SETTING (22, 23, Importance.DEBUG, BadAISInternalSettingException.class),
    TYPES_ARE_STATIC     (22, 24, Importance.DEBUG, TypesAreStaticException.class),
    
    // Bad Type errors
    UNKNOWN_TYPE_SIZE    (22, 200, Importance.DEBUG, UnknownTypeSizeException.class),
    UNKNOWN_TYPE         (22, 201, Importance.DEBUG, UnknownDataTypeException.class),
    INCONVERTIBLE_TYPES  (22, 202, Importance.DEBUG, InconvertibleTypesException.class),
    
    // Session state errors 
    NO_SUCH_SCHEMA          (23,  1, Importance.DEBUG, NoSuchSchemaException.class),
    // Unsupported Features Errors, Should be empty, but isn't
    UNSUPPORTED_SQL         (28, 0, Importance.ERROR, UnsupportedSQLException.class),
    UNSUPPORTED_PARAMETERS  (28, 1, Importance.ERROR, UnsupportedParametersException.class),
    UNSUPPORTED_EXPLAIN     (28, 2, Importance.ERROR, UnsupportedExplainException.class),
    UNSUPPORTED_CREATE_SELECT (28, 3, Importance.ERROR, UnsupportedCreateSelectException.class),
    UNSUPPORTED_FK_INDEX    (28, 4, Importance.ERROR, UnsupportedFKIndexException.class),
    UNSUPPORTED_CHECK       (28, 5, Importance.ERROR, UnsupportedCheckConstraintException.class),
    UNSUPPORTED_GROUP_UNIQUE(28, 6, Importance.DEBUG, UnsupportedUniqueGroupIndexException.class),
    UNSUPPORTED_INDEX_PREFIX(28, 7, Importance.ERROR, UnsupportedIndexPrefixException.class),
    
    // Configuration, Startup, & Shutdown errors
    SERVICE_NOT_STARTED  (29, 1, Importance.ERROR, ServiceNotStartedException.class),
    SERVICE_ALREADY_STARTED (29, 2, Importance.ERROR, ServiceStartupException.class),
    SERVICE_CIRC_DEPEND  (29, 3, Importance.ERROR, CircularDependencyException.class),
    BAD_ADMIN_DIRECTORY  (29, 4, Importance.ERROR, BadAdminDirectoryException.class),
    ZOOKEEPER_INIT_FAIL  (29, 5, Importance.ERROR, ZooKeeperInitFailureException.class),
    CONFIG_LOAD_FAILED   (29, 6, Importance.ERROR, ConfigurationPropertiesLoadException.class),
    THREAD_START_INTR    (29, 7, Importance.ERROR, ThreadStartInterruptedException.class),
    THREAD_STOP_INTR     (29, 8, Importance.DEBUG, ThreadStopInterruptedException.class),
    NET_START_IO_ERROR   (29, 9, Importance.ERROR, NetworkStartIOException.class),
    NET_STOP_IO_ERROR    (29, 10, Importance.ERROR, NetworkStopIOException.class),
    TAP_BEAN_FAIL        (29, 11, Importance.ERROR, TapBeanFailureException.class),
    SET_FILTER_FAIL      (29, 12, Importance.ERROR, DisplayFilterSetException.class),
    SCHEMA_LOAD_IO_ERROR (29, 13, Importance.ERROR, SchemaLoadIOException.class),
    QUERY_LOG_CLOSE_FAIL (29, 14, Importance.ERROR, QueryLogCloseException.class),
    INVALID_PORT         (29, 15, Importance.ERROR, InvalidPortException.class), 
    
    // AkSserver errors
    MULTIGENERATIONAL_TABLE(30, 900, Importance.ERROR, null),

    // Bad AkSserver errors
    INTERNAL_ERROR      (31, 0, Importance.ERROR, null),
    INTERNAL_CORRUPTION (31, 1, Importance.ERROR, RowDataCorruptionException.class),
    AIS_TOO_LARGE       (31, 2, Importance.ERROR, AISTooLargeException.class),
    CURSOR_IS_FINISHED  (31, 3, Importance.ERROR, CursorIsFinishedException.class), 
    CURSOR_IS_UNKNOWN   (31, 4, Importance.ERROR, CursorIsUnknownException.class),
    NO_ACTIVE_CURSOR    (31, 5, Importance.ERROR, NoActiveCursorException.class),
    CURSOR_CLOSE_BAD    (31, 6, Importance.ERROR, CursorCloseBadException.class),
    PERSISTIT_ERROR     (31, 7, Importance.ERROR, PersistItErrorException.class),
    INVALID_VOLUME      (31, 8, Importance.ERROR, InvalidVolumeException.class),
    TABLE_NOT_BOUND     (31, 9, Importance.ERROR, TableNotBoundException.class),
    ;

    private final short value;
    private final Importance importance;
    //private final String message; 
    private final Class<? extends InvalidOperationException> exceptionClass;
    private String formattedValue; 
    private static ResourceBundle resourceBundle = ResourceBundle.getBundle("com.akiban.server.error.error_code");

    
    static short computeShort(int groupValue, int subCode) {
        if (groupValue < 0 || groupValue > 31) {
            throw new RuntimeException("invalid group value: " + groupValue);
        }
        if (subCode < 0 || subCode > 999) {
            throw new RuntimeException("invalid subcode value: " + subCode);
        }
        return (short) (groupValue * 1000 + subCode);
    }
    
    private ErrorCode(int groupValue, int subCode, Importance importance, 
            Class<? extends InvalidOperationException> exception) {
        this.value = computeShort(groupValue, subCode);
        this.importance = importance;
        //this.message = message;
        this.exceptionClass = exception;
        this.formattedValue = String.format("%02d%03d", groupValue, subCode); 
    }

    public static ErrorCode valueOf(short value)
    {
        for (ErrorCode e : values()) {
            if (e.getShort() == value) {
                return e;
            }
        }
        throw new RuntimeException(String.format("Invalid code value: %s", value));
    }

    public short getShort() {
        return value;
    }

    public Importance getImportance() {
        return importance;
    }
    
    public String getMessage() { 
        return resourceBundle.getString(name());
    }
    public Class<? extends InvalidOperationException> associatedExceptionClass() {
        return exceptionClass; 
    }
    public String getFormattedValue() {
        return formattedValue;
    }

    public static enum Importance {
        TRACE,
        DEBUG,
        ERROR
    }
}
