/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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

import com.foundationdb.server.types.aksql.aktypes.AkBlob;

import java.util.ArrayList;
import java.util.List;


public class GroupsContainsLobsVisitor extends AbstractVisitor {

    private List<Table> collection = new ArrayList<>();
    private boolean containsLob = false;
    
    @Override
    public void visit(Column column) {
        if (AkBlob.isBlob(column.getType().typeClass())) {
            if (!collection.contains(column.getTable().getGroup())) {
                collection.add(column.getTable());
                containsLob = true;
            }
        }
    }
    
    public List<Table> getCollection() {
        return collection;
    }
    
    public boolean containsLob() {
        return containsLob;
    }
    
}
