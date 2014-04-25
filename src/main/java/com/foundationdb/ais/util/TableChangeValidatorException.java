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

package com.foundationdb.ais.util;

import static java.lang.String.format;

public class TableChangeValidatorException extends IllegalArgumentException {
    private static final String COLUMN = "column";
    private static final String INDEX = "index";
    private static final String ADD_NOT_PRESENT_MSG = "ADD %s not in new table: %s";
    private static final String DROP_NOT_PRESENT_MSG = "DROP %s not in old table: %s";
    private static final String MODIFY_NOT_PRESENT_MSG = "MODIFY %s not in old or new table: %s";
    private static final String MODIFY_NOT_CHANGED_MSG = "MODIFY %s not changed: %s";
    private static final String UNCHANGED_NOT_PRESENT_MSG = "Unchanged %s not present in new table: %s";
    private static final String UNDECLARED_CHANGE_MSG = "Undeclared %s change in new table: %s";

    public TableChangeValidatorException(String detail) {
        super(detail);
    }

    //
    // Column
    //

    public static class AddColumnNotPresentException extends TableChangeValidatorException {
        public AddColumnNotPresentException(String detail) {
            super(format(ADD_NOT_PRESENT_MSG, COLUMN, detail));
        }
    }

    public static class DropColumnNotPresentException extends TableChangeValidatorException {
        public DropColumnNotPresentException(String detail) {
            super(format(DROP_NOT_PRESENT_MSG, COLUMN, detail));
        }
    }

    public static class ModifyColumnNotPresentException extends TableChangeValidatorException {
        public ModifyColumnNotPresentException(String detail) {
            super(format(MODIFY_NOT_PRESENT_MSG, COLUMN, detail));
        }
    }

    public static class ModifyColumnNotChangedException extends TableChangeValidatorException {
        public ModifyColumnNotChangedException(String detail) {
            super(format(MODIFY_NOT_CHANGED_MSG, COLUMN, detail));
        }
    }

    public static class UnchangedColumnNotPresentException extends TableChangeValidatorException {
        public UnchangedColumnNotPresentException(String detail) {
            super(format(UNCHANGED_NOT_PRESENT_MSG, COLUMN, detail));
        }
    }

    public static class UndeclaredColumnChangeException extends TableChangeValidatorException {
        public UndeclaredColumnChangeException(String detail) {
            super(format(UNDECLARED_CHANGE_MSG, COLUMN, detail));
        }
    }

    //
    // Index
    //

    public static class AddIndexNotPresentException extends TableChangeValidatorException {
        public AddIndexNotPresentException(String detail) {
            super(format(ADD_NOT_PRESENT_MSG, INDEX, detail));
        }
    }

    public static class DropIndexNotPresentException extends TableChangeValidatorException {
        public DropIndexNotPresentException(String detail) {
            super(format(DROP_NOT_PRESENT_MSG, INDEX, detail));
        }
    }

    public static class ModifyIndexNotPresentException extends TableChangeValidatorException {
        public ModifyIndexNotPresentException(String detail) {
            super(format(MODIFY_NOT_PRESENT_MSG, INDEX, detail));
        }
    }

    public static class ModifyIndexNotChangedException extends TableChangeValidatorException {
        public ModifyIndexNotChangedException(String detail) {
            super(format(MODIFY_NOT_CHANGED_MSG, INDEX, detail));
        }
    }
}
