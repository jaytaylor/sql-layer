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

package com.foundationdb.sql.server;

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
        REQUIRED_WRITE,         // Must have read write transaction.
        IMPLICIT_COMMIT,        // Automatically commit an open transaction
        IMPLICIT_COMMIT_AND_NEW;
    }

    public enum TransactionAbortedMode {
        ALLOWED,                // Statement always allowed
        NOT_ALLOWED,            // Statement never allowed
    }

    public enum AISGenerationMode {
        ALLOWED,               // Statement can be used under any generation
        NOT_ALLOWED            // Statement can only be used under one generation
    }

    public TransactionMode getTransactionMode();
    public TransactionAbortedMode getTransactionAbortedMode();
    public AISGenerationMode getAISGenerationMode();
}
