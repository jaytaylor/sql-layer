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
    void visitType(Type type);
    void visitGroup(Group group);
    void visitUserTable(UserTable userTable);
    void visitGroupTable(GroupTable groupTable);
    void visitColumn(Column column);
    void visitJoin(Join join) ;
    void visitJoinColumn(JoinColumn joinColumn);
    void visitIndex(Index index);
    void visitIndexColumn(IndexColumn indexColumn);
}
