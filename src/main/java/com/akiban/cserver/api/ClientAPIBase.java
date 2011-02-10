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

package com.akiban.cserver.api;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.common.NoSuchTableException;
import com.akiban.cserver.api.ddl.DuplicateTableNameException;
import com.akiban.cserver.api.ddl.JoinToUnknownTableException;
import com.akiban.cserver.api.ddl.JoinToWrongColumnsException;
import com.akiban.cserver.api.ddl.ParseException;
import com.akiban.cserver.api.ddl.ProtectedTableDDLException;
import com.akiban.cserver.api.ddl.UnsupportedCharsetException;
import com.akiban.cserver.api.ddl.UnsupportedDataTypeException;
import com.akiban.cserver.api.ddl.UnsupportedDropException;
import com.akiban.cserver.api.dml.DuplicateKeyException;
import com.akiban.cserver.api.dml.ForeignKeyConstraintDMLException;
import com.akiban.cserver.api.dml.NoSuchColumnException;
import com.akiban.cserver.api.dml.NoSuchIndexException;
import com.akiban.cserver.api.dml.NoSuchRowException;
import com.akiban.cserver.api.dml.UnsupportedModificationException;
import com.akiban.cserver.api.dml.scan.CursorIsFinishedException;
import com.akiban.cserver.api.dml.scan.CursorIsUnknownException;
import com.akiban.cserver.service.ServiceManager;
import com.akiban.cserver.service.ServiceManagerImpl;
import com.akiban.cserver.store.SchemaManager;
import com.akiban.cserver.store.Store;
import com.akiban.cserver.util.RowDefNotFoundException;

abstract class ClientAPIBase {

    private final Store store;
    private final SchemaManager schemaManager;

    ClientAPIBase() {
        final ServiceManager serviceManager = ServiceManagerImpl.get();
        this.store = serviceManager.getStore();
        this.schemaManager = serviceManager.getSchemaManager();
    }

    final public Store store() {
        return store;
    }

    final public SchemaManager schemaManager() {
        return schemaManager;
    }

    /**
     * Returns an exception as an InvalidOperationException. If the given
     * exception is one that we know how to turn into a specific
     * InvalidOperationException (e.g., NoSuchRowException), the returned
     * exception will be of that type. Otherwise, if the given exception is an
     * InvalidOperationException, we'll just return it, and if not, we'll wrap
     * it in a GenericInvalidOperationException.
     * 
     * @param e
     *            the exception to wrap
     * @return as specific an InvalidOperationException as we know how to make
     */
    protected static InvalidOperationException launder(Exception e) {
        if (e instanceof InvalidOperationException) {
            final InvalidOperationException ioe = (InvalidOperationException) e;
            switch (ioe.getCode()) {
                case NO_SUCH_RECORD:
                    return new NoSuchRowException(ioe);
                case PARSE_EXCEPTION:
                    return new ParseException(ioe);
                case DUPLICATE_KEY:
                    return new DuplicateKeyException(ioe);

                case UNSUPPORTED_CHARSET:
                    return new UnsupportedCharsetException(ioe);
                case PROTECTED_TABLE:
                    return new ProtectedTableDDLException(ioe);
                case JOIN_TO_PROTECTED_TABLE:
                    return new ProtectedTableDDLException(ioe);
                case JOIN_TO_UNKNOWN_TABLE:
                    return new JoinToUnknownTableException(ioe);
                case JOIN_TO_WRONG_COLUMNS:
                    return new JoinToWrongColumnsException(ioe);
                case DUPLICATE_TABLE:
                    return new DuplicateTableNameException(ioe);
                case UNSUPPORTED_DROP:
                    return new UnsupportedDropException(ioe);
                case UNSUPPORTED_DATA_TYPE:
                    return new UnsupportedDataTypeException(ioe);
                case NO_SUCH_TABLE:
                    return new NoSuchTableException(ioe);
                case NO_SUCH_COLUMN:
                    return new NoSuchColumnException(ioe);
                case NO_INDEX:
                    return new NoSuchIndexException(ioe);
                case FK_CONSTRAINT_VIOLATION:
                    return new ForeignKeyConstraintDMLException(ioe);
                case NO_SUCH_ROW:
                    return new NoSuchRowException(ioe);
                case CURSOR_IS_FINISHED:
                    return new CursorIsFinishedException(ioe);
                case CURSOR_IS_UNKNOWN:
                    return new CursorIsUnknownException(ioe);
                case UNSUPPORTED_MODIFICATION:
                    return new UnsupportedModificationException(ioe);
                default:
                    return ioe;
            }
        }
        if (e instanceof RowDefNotFoundException) {
            return new NoSuchTableException( ((RowDefNotFoundException)e).getId() );
        }
        return new GenericInvalidOperationException(e);
    }

    /**
     * Throws the given InvalidOperationException, downcast, if it's of the
     * appropriate type
     * 
     * @param cls
     *            the class to check for and cast to
     * @param e
     *            the exception to check
     * @param <T>
     *            an InvalidOperationException to throw as
     * @throws T
     *             the e instance, cast down
     */
    protected static <T extends InvalidOperationException> void throwIfInstanceOf(
            Class<T> cls, InvalidOperationException e) throws T {
        if (cls.isInstance(e)) {
            throw cls.cast(e);
        }
    }
}
