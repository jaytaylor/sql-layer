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

package com.akiban.qp;

public interface GroupCursor extends Cursor
{
    /**
     * Starts a scan of the underlying table that will visit the row matching the given hKey
     * and all descendent rows. Descendents will be scanned even if there is no ancestor matching the hkey,
     * (i.e., orphan rows are scanned).
     *
     * @param hKey Limits the scan to rows matching this hKey, and descendents.
     * @throws UnsupportedOperationException if applied to an index-based cursor.
     */
    void open(HKey hKey);
}
