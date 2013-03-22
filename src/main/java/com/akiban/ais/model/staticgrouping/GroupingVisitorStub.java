/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.ais.model.staticgrouping;

import java.util.List;

import com.akiban.ais.model.TableName;

public abstract class GroupingVisitorStub<T> implements GroupingVisitor<T> {
    @Override
    public void start(String defaultSchema) {
    }

    @Override
    public void visitGroup(Group group, TableName rootTable) {
    }

    @Override
    public void finishGroup() {
    }

    @Override
    public void visitChild(TableName parentName, List<String> parentColumns, TableName childName, List<String> childColumns) {
    }

    @Override
    public boolean startVisitingChildren() {
        return true;
    }

    @Override
    public void finishVisitingChildren() {
    }
}
