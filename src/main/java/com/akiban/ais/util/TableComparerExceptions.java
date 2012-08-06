/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.ais.util;

import static java.lang.String.format;

public class TableComparerExceptions {
    private static final String COLUMN = "column";
    private static final String INDEX = "index";
    private static final String ADD_NOT_PRESENT_MSG = "ADD %s not in new table: %s";
    private static final String DROP_NOT_PRESENT_MSG = "DROP %s not in old table: %s";
    private static final String MODIFY_NOT_PRESENT_MSG = "MODIFY %s not in old or new table: %s";
    private static final String MODIFY_NOT_CHANGED_MSG = "MODIFY %s not changed: %s";
    private static final String UNCHANGED_NOT_PRESENT_MSG = "Unchanged %s not present in new table: %s";
    private static final String UNDECLARED_CHANGE_MSG = "Undeclared %s change in new table: %s";

    //
    // Column
    //

    public static class AddColumnNotPresentException extends RuntimeException {
        public AddColumnNotPresentException(String detail) {
            super(format(ADD_NOT_PRESENT_MSG, COLUMN, detail));
        }
    }

    public static class DropColumnNotPresentException extends RuntimeException {
        public DropColumnNotPresentException(String detail) {
            super(format(DROP_NOT_PRESENT_MSG, COLUMN, detail));
        }
    }

    public static class ModifyColumnNotPresentException extends RuntimeException {
        public ModifyColumnNotPresentException(String detail) {
            super(format(MODIFY_NOT_PRESENT_MSG, COLUMN, detail));
        }
    }

    public static class ModifyColumnNotChangedException extends RuntimeException {
        public ModifyColumnNotChangedException(String detail) {
            super(format(MODIFY_NOT_CHANGED_MSG, COLUMN, detail));
        }
    }

    public static class UnchangedColumnNotPresentException extends RuntimeException {
        public UnchangedColumnNotPresentException(String detail) {
            super(format(UNCHANGED_NOT_PRESENT_MSG, COLUMN, detail));
        }
    }

    public static class UndeclaredColumnChangeException extends RuntimeException {
        public UndeclaredColumnChangeException(String detail) {
            super(format(UNDECLARED_CHANGE_MSG, COLUMN, detail));
        }
    }

    //
    // Index
    //

    public static class AddIndexNotPresentException extends RuntimeException {
        public AddIndexNotPresentException(String detail) {
            super(format(ADD_NOT_PRESENT_MSG, INDEX, detail));
        }
    }

    public static class DropIndexNotPresentException extends RuntimeException {
        public DropIndexNotPresentException(String detail) {
            super(format(DROP_NOT_PRESENT_MSG, INDEX, detail));
        }
    }

    public static class ModifyIndexNotPresentException extends RuntimeException {
        public ModifyIndexNotPresentException(String detail) {
            super(format(MODIFY_NOT_PRESENT_MSG, INDEX, detail));
        }
    }

    public static class ModifyIndexNotChangedException extends RuntimeException {
        public ModifyIndexNotChangedException(String detail) {
            super(format(MODIFY_NOT_CHANGED_MSG, INDEX, detail));
        }
    }

    public static class UnchangedIndexNotPresentException extends RuntimeException {
        public UnchangedIndexNotPresentException(String detail) {
            super(format(UNCHANGED_NOT_PRESENT_MSG, INDEX, detail));
        }
    }

    public static class UndeclaredIndexChangeException extends RuntimeException {
        public UndeclaredIndexChangeException(String detail) {
            super(format(UNDECLARED_CHANGE_MSG, INDEX, detail));
        }
    }


    private TableComparerExceptions()
    {}
}
