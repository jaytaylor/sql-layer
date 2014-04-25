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

package com.foundationdb.server.error;

import com.foundationdb.ais.model.HasStorage;

public class DuplicateStorageDescriptionKeysException extends InvalidOperationException {
    public DuplicateStorageDescriptionKeysException (HasStorage obj1, HasStorage obj2, Object key) {
        super (ErrorCode.DUPLICATE_STORAGE_DESCRIPTION_KEYS,
               obj1.getTypeString(), obj1.getNameString(),
               obj2.getTypeString(), obj2.getNameString(),
               key.toString());
    }
}
