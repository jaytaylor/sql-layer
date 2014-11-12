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

package com.foundationdb.ais.model;

import com.foundationdb.ais.util.TableChange;

import java.util.List;

/** Attached to a <code>Table</code> on which <code>ALTER</code> has been performed
 * by <a href="http://www.percona.com/doc/percona-toolkit/2.1/pt-online-schema-change.html">pt-online-schema-change.html</a>. 
 * The same alter will be done to the <code>originalName</code> when a
 * <code>RENAME</code> is requested after all the row copying.
 */
public class PendingOSC
{
    private String originalName, currentName;
    private List<TableChange> columnChanges, indexChanges;

    public PendingOSC(String originalName, List<TableChange> columnChanges, List<TableChange> indexChanges) {
        this.originalName = originalName;
        this.columnChanges = columnChanges;
        this.indexChanges = indexChanges;
    }

    public String getOriginalName() {
        return originalName;
    }

    public String getCurrentName() {
        return currentName;
    }

    public void setCurrentName(String currentName) {
        this.currentName = currentName;
    }

    public List<TableChange> getColumnChanges() {
        return columnChanges;
    }

    public List<TableChange> getIndexChanges() {
        return indexChanges;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(originalName);
        if (currentName != null)
            str.append("=").append(currentName);
        str.append(columnChanges).append(indexChanges);
        return str.toString();
    }    
}
