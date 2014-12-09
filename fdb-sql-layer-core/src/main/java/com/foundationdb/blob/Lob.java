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

package com.foundationdb.blob;

import com.foundationdb.*;
import com.foundationdb.directory.*;
import com.foundationdb.async.Future;

import java.util.UUID;

public interface Lob {
    public UUID getId();
    public Future<Long> getSize(TransactionContext tcx);
    public DirectorySubspace getDirectorySubspace();
    public Future<Void> delete(TransactionContext tcx);
    public Future<Void> truncate(TransactionContext tcx, Long len);
}
