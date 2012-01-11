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

package com.akiban.sql.server;

/**
 * An executable statement. Base only handles transactionality, not
 * actual execution and returning of values.
 */
public interface ServerStatement
{
    /** What transaction mode(s) does this statement use? */
    public enum TransactionMode { 
        ALLOWED,                // Does not matter.
        NONE,                   // Must not have a transaction; none created.
        NEW,                    // Must not have a transaction: read only created.
        NEW_WRITE,              // Must not have a transaction: read write created.
        READ,                   // New read only or existing allowed.
        WRITE,                  // New or existing read write allowed.
        REQUIRED,               // Must have transaction: read only okay.
        REQUIRED_WRITE          // Must have read write transaction.
    };

    public TransactionMode getTransactionMode();

}
