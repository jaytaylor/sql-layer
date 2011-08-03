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
    UNKNOWN               (0, 0, Importance.ERROR, null, "UNKNOWN: Unused error code"),
    UNEXPECTED_EXCEPTION  (0, 1, Importance.ERROR, null, "UNEXPECTED EXCEPTION: unused error code"),
    UNSUPPORTED_OPERATION (0, 900, Importance.ERROR, null, "UNSUPPORTED_OPERATION: unused error code"),

    // AkSserver and Head are out of sync
    SERVER_SHUTDOWN         (1, 0, Importance.DEBUG, null, "SERVER_SHUTDOWN: Unused Error Code"),
    STALE_AIS               (1, 1, Importance.TRACE, OldAISException.class, "AIS generation seen was %d but current generation is %d"), //OldAISException
    METAMODEL_MISMATCH      (1, 2, Importance.ERROR, MetaModelVersionMismatchException.class, "AIS Model version mismatch, expected version %d vs actual version %d"),

    // DDL errors
    PARSE_EXCEPTION         (2, 0, Importance.DEBUG, ParseException.class, "[%s] %s: %s"),
    UNSUPPORTED_CHARSET     (2, 1, Importance.DEBUG, UnsupportedCharsetException.class, "Table `%s`.`%s` has unsupported character set `%s`"), 
    PROTECTED_TABLE         (2, 2, Importance.DEBUG, ProtectedTableDDLException.class, "Cannot create or modify table `%s` in protected schema `%s`"), 
    JOIN_TO_PROTECTED_TABLE (2, 3, Importance.DEBUG, JoinToProtectedTableException.class, "Table `%s`.`%s` joins to protected table `%s`.`%s`"), 
    JOIN_TO_UNKNOWN_TABLE   (2, 4, Importance.DEBUG, JoinToUnknownTableException.class, "Table `%s`.`%s` joins to undefined table `%s`.`%s`"),  
    JOIN_TO_WRONG_COLUMNS   (2, 5, Importance.DEBUG, JoinToWrongColumnsException.class, "Table `%s`.`%s` join reference part `%s` does not match `%s`.`%s` primary key part `%s`"), 
    DUPLICATE_TABLE         (2, 6, Importance.DEBUG, DuplicateTableNameException.class, "Table `%s`.`%s` already exists"), 
    UNSUPPORTED_DROP        (2, 7, Importance.DEBUG, UnsupportedDropException.class, "Cannot drop non-leaf table `%s`.`%s`"),
    UNSUPPORTED_DATA_TYPE   (2, 8, Importance.DEBUG, UnsupportedDataTypeException.class, "Table `%s`.`%s` has column `%s` with unsupported data type `%s`"),
    JOIN_TO_MULTIPLE_PARENTS(2, 9, Importance.DEBUG, JoinToMultipleParentsException.class, "Table `%s`.`%s` has joins to two parents"), 
    UNSUPPORTED_INDEX_DATA_TYPE(2, 10, Importance.DEBUG, UnsupportedIndexDataTypeException.class, "Table `%s`.`%s` index `%s` has unsupported type `%s` from column `%s`"),
    UNSUPPORTED_INDEX_SIZE  (2, 11, Importance.DEBUG, UnsupportedIndexSizeException.class, "Table `%s`.`%s` index `%s` exceeds maximum key size"),
    DUPLICATE_COLUMN        (2, 12, Importance.DEBUG, DuplicateColumnNameException.class, "Table `%s`.`%s` already has column `%s`"),
    DUPLICATE_GROUP         (2, 13, Importance.DEBUG, DuplicateGroupNameException.class, "Group `%s` already exists"), 
    REFERENCED_TABLE        (2, 14, Importance.DEBUG, ReferencedTableException.class,  "Table %s has one or more child tables in the group"),  
    DROP_INDEX_NOT_ALLOWED  (2, 15, Importance.DEBUG, DropIndexNotAllowedException.class,  "Can not drop index `%s` from table `%s`.`%s`"),
    FK_DDL_VIOLATION        (2, 16, Importance.DEBUG, ForeignConstraintDDLException.class, "Cannot drop table `%s`.`%s`, it has child table `%s`.`%s`"),
    PROTECTED_INDEX         (2, 17, Importance.DEBUG, ProtectedIndexException.class, "Index `%s` can not be added to a table"),
    BRANCHING_GROUP_INDEX   (2, 18, Importance.DEBUG, BranchingGroupIndexException.class, "Group index `%s` has tables %s%s and %s%s on different branches"),

    // DML errors
    NO_REFERENCED_ROW (3, 0, Importance.DEBUG, null,  "NO_REFERENCED_ROW: Unused error code"), // NULL
    DUPLICATE_KEY     (3, 1, Importance.DEBUG, DuplicateKeyException.class, "Non-unique key for index %s: %s"),
    NO_SUCH_TABLE     (3, 2, Importance.DEBUG, NoSuchTableException.class,  "Can not find the table `%s`.`%s`"), 

    NO_INDEX          (3, 4, Importance.DEBUG, NoSuchIndexException.class, "Unknown index: `%s`"),
    NO_SUCH_RECORD    (3, 5, Importance.DEBUG, null,  "NO_SUCH_RECORD: Unused error code"),
    FK_CONSTRAINT_VIOLATION(3, 6, Importance.DEBUG, ForeignKeyConstraintDMLException.class, "FK_CONSTRAINT_VIOLATION: Unused error code"),
    UNSUPPORTED_MODIFICATION(3, 7, Importance.DEBUG, null, "UNSUPPORTED_MODIFICATION: Unused error code"),
//    UNSUPPORTED_READ(3, 8), TODO: used to mean a multi-branch, non-DEEP scan request. Still valid error code?
    TABLEDEF_MISMATCH (3, 9, Importance.DEBUG, TableDefinitionMismatchException.class, "ID<%d> from RowData didn't match given ID <%d>"), 
    NO_SUCH_ROW       (3, 10, Importance.DEBUG,  NoSuchRowException.class, "Missing record at key: %s"),
    CONCURRENT_MODIFICATION(3, 11, Importance.DEBUG, ConcurrentScanAndUpdateException.class, "Update concurrent with scan for %s"), 
    TABLE_DEFINITION_CHANGED(3, 12, Importance.DEBUG, TableDefinitionChangedException.class, "Update concurrent with table definition change for %s"),
    NO_SUCH_GROUP     (3, 13, Importance.DEBUG, NoSuchGroupException.class,        "No group with name %s"), 
    NO_SUCH_TABLEDEF  (3, 14, Importance.DEBUG, RowDefNotFoundException.class, "Can not find table definition for id <%d>"), 
    NO_ROWS_UPDATED   (3, 15, Importance.DEBUG, NoRowsUpdatedException.class,  "Update row did not update any rows: %s"),    
    TOO_MANY_ROWS_UPDATED (3, 16, Importance.DEBUG, TooManyRowsUpdatedException.class, "Update row touched %d, altered %d rows: %s"),  
    NO_SUCH_TABLEID   (3, 17, Importance.DEBUG, NoSuchTableIdException.class, "Can not find a table by ID<%d>"),
    SCAN_RETRY_ABANDONDED (3, 18, Importance.ERROR, ScanRetryAbandonedException.class, "Scan abandonded after <%d> retries"),
    
    ROW_OUTPUT(4, 11, Importance.DEBUG, RowOutputException.class, "Buffer too small to accomodate row output. Row count: %d"), 

    // Messaging errors
    MALFORMED_REQUEST (21, 0, Importance.ERROR, null, "MALFORMED_REQUEST: Unused error code"), 
    BAD_STATISTICS_TYPE (21, 4, Importance.ERROR, BadStatisticsTypeException.class, "Unexpected histogram request type <%d>"),
    
    // AIS Validation errors, Attempts to modify and build an AIS failed
    // due to missing or invalid information.
    VALIDATION_FAILURE(22, 0, Importance.DEBUG, null, "VALIDATION_FAILURE: Unused error code"),
    INTERNAL_REFERENCES_BROKEN(22, 1, Importance.DEBUG, null, "INTERNAL_REFERENCES_BROKEN: Unused error code"),
    GROUP_MULTIPLE_ROOTS (22,  2, Importance.DEBUG, GroupHasMultipleRootsException.class,   "Group `%s` has multiple root tables: `%s`.`%s` and `%s`.`%s`"),
    JOIN_TYPE_MISMATCH   (22,  3, Importance.DEBUG, JoinColumnTypesMismatchException.class, "Column `%s`.`%s`.`%s` type used for join does not match parent column `%s`.`%s`.`%s`"),
    PK_NULL_COLUMN       (22,  4, Importance.DEBUG, PrimaryKeyNullColumnException.class, "Table `%s`.`%s` primary key has a nullable column %s"),
    DUPLICATE_INDEXES    (22,  5, Importance.DEBUG, DuplicateIndexException.class,     "Table `%s`.`%s` already has index `%s`"),
    MISSING_PRIMARY_KEY  (22,  6, Importance.DEBUG, NoPrimaryKeyException.class,       "Table `%s`.`%s` is missing its primary key"),
    DUPLICATE_TABLEID    (22,  7, Importance.DEBUG, DuplicateTableIdException.class,   "Table `%s`.`%s` has a duplicate tableID to table `%s`.`%s`"),
    JOIN_COLUMN_MISMATCH (22,  8, Importance.DEBUG, JoinColumnMismatchException.class, "Join column list size (%d) for table `%s`.`%s` does not match table `%s`.`%s` primary key (%d)"),
    INDEX_LACKS_COLUMNS  (22,  9, Importance.DEBUG, IndexLacksColumnsException.class,  "Table `%s`.`%s` index `%s` defined without any colums"),
    NO_SUCH_COLUMN       (22, 10, Importance.DEBUG, NoSuchColumnException.class,       "Unknown column: %s"),
    DUPLICATE_INDEX_TREENAME (22, 11, Importance.DEBUG, DuplicateIndexTreeNamesException.class, "Index `%s`.`%s`.`%s` has duplicate tree name to index `%s`.`%s`.`%s`. Treename: [%s]"),
    DUPLICATE_TABLE_TREENAME (22, 12, Importance.DEBUG, DuplicateTableTreeNamesException.class, "Table `%s`.`%s` has duplicate tree name to table `%s`.`%s`. Treename: [%s]"),
    TABLE_NOT_IN_GROUP   (22, 13, Importance.DEBUG, TableNotInGroupException.class,          "Table `%s`.`%s` does not belong to any group"),
    NAME_IS_NULL         (22, 14, Importance.DEBUG, NameIsNullException.class,               "Attempting to create %s with a null %s"),
    DUPLICATE_INDEX_COLUMN (22, 15, Importance.DEBUG, DuplicateIndexColumnException.class,   "Index `%s`.`%s`.`%s` has duplicate column `%s`"),
    COLUMN_POS_ORDERED   (22, 16, Importance.DEBUG, ColumnPositionNotOrderedException.class, "table `%s`.`%s` has column `%s` in position <%d> which is out of index order <%d>"),
    TABLE_COL_IN_GROUP   (22, 17, Importance.DEBUG, TableColumnNotInGroupException.class,    "Table `%s`.`%s` column `%s` is not in Group table"),
    GROUP_MISSING_COL    (22, 18, Importance.DEBUG, GroupMissingTableColumnException.class,  "Group table `%s`.`%s` is missing user table column `%s`.`%s`.`%s`"),
    GROUP_MISSING_INDEX  (22, 19, Importance.DEBUG, GroupMissingIndexException.class,        "Group table `%s`.`%s` is missing user table index `%s`"),
    TREENAME_MISMATCH    (22, 20, Importance.DEBUG, TreeNameMismatchException.class,         "Table `%s`.`%s` treename `%s` does not match its group table treename %s"),
    NULL_REFERENCE       (22, 21, Importance.DEBUG, AISNullReferenceException.class,         "%s `%s` has null reference to %s"),
    BAD_AIS_REFERENCE    (22, 22, Importance.DEBUG, BadAISReferenceException.class,          "%s `%s` has bad reference to %s `%s`"),
    BAD_INTERNAL_SETTING (22, 23, Importance.DEBUG, BadAISInternalSettingException.class,    "%s `%s` has incorrect internal setting for %s"),
    // AkSserver errors
    // TRANSACTION_ERROR(30, 0), // TODO PersistitException should get caught and wrapped as this
    MULTIGENERATIONAL_TABLE(30, 900, Importance.ERROR, null, "MULTIGENERATION_TABLE: Unused error code"),

    // Bad AkSserver errors
    INTERNAL_ERROR      (31, 0, Importance.ERROR, null, "INTERNAL_ERROR: Unused error code"),
    INTERNAL_CORRUPTION (31, 1, Importance.ERROR, RowDataCorruptionException.class, "Internal corrupt RowData at %s"),
    AIS_TOO_LARGE       (31, 2, Importance.ERROR, AISTooLargeException.class, "Serialized AIS size <%d> exceeds maximum size<%d>"),
    CURSOR_IS_FINISHED  (31, 3, Importance.ERROR, CursorIsFinishedException.class, "Finished scan cursor requested more rows: %s"), 
    CURSOR_IS_UNKNOWN   (31, 4, Importance.ERROR, CursorIsUnknownException.class,  "Cursor/RowCollector for Table <%d> is unknown"),
    NO_ACTIVE_CURSOR    (31, 5, Importance.ERROR, NoActiveCursorException.class,   "No cursor found for tableId <%d>"),
    CURSOR_CLOSE_BAD    (31, 6, Importance.ERROR, CursorCloseBadException.class,   "Removing RowCollector for Table <%d> remove wrong one"),
    PERSISTIT_ERROR     (31, 7, Importance.ERROR, PersistItErrorException.class,    "Persistit Data Layer error: %s")
    ;

    private final short value;
    private final Importance importance;
    private final String message; 
    private final Class<? extends InvalidOperationException> exceptionClass;
    
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
            Class<? extends InvalidOperationException> exception, String message) {
        this.value = computeShort(groupValue, subCode);
        this.importance = importance;
        this.message = message;
        this.exceptionClass = exception;
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
        return message;
    }
    public Class<? extends InvalidOperationException> associatedExceptionClass() {
        return exceptionClass; 
    }

    public static enum Importance {
        TRACE,
        DEBUG,
        ERROR
    }
}
