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

public class GroupIndex extends Index
{
    public static GroupIndex create(AkibanInformationSchema ais, Group group, String indexName, Integer indexId,
                                    Boolean isUnique, String constraint)
    {
        GroupIndex index = new GroupIndex(group, indexName, indexId, isUnique, constraint);
        group.addIndex(index);
        return index;
    }

    public GroupIndex(Group group, String indexName, Integer indexId, Boolean isUnique, String constraint)
    {
        super(new TableName("", group.getName()), indexName, indexId, isUnique, constraint);
        this.group = group;
    }

    @Override
    public boolean isTableIndex()
    {
        return false;
    }

    public Group getGroup()
    {
        return group;
    }

    @Override
    public HKey hKey()
    {
        // TODO: Implement or refactor
        return null;
    }

    private Group group;
}
