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

package com.akiban.ais.model;

public interface Visitor
{
    void visitType(Type type) throws Exception;
    void visitGroup(Group group) throws Exception;
    void visitUserTable(UserTable userTable) throws Exception;
    void visitGroupTable(GroupTable groupTable) throws Exception;
    void visitColumn(Column column) throws Exception;
    void visitJoin(Join join) throws Exception;
    void visitJoinColumn(JoinColumn joinColumn) throws Exception;
    void visitIndex(Index index) throws Exception;
    void visitIndexColumn(IndexColumn indexColumn) throws Exception;
}
