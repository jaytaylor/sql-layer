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

package com.akiban.ais.ddl;

import com.akiban.ais.model.staticgrouping.GroupsBuilder;

public final class GroupDef {
    private GroupsBuilder groupsBuilder;

    public void seeHeaderSchema(String text) {
        groupsBuilder = new GroupsBuilder(text);
    }

    public void seeHeaderBaseid(String text) {
        assert false : "saw baseid=" + text;
    }

    public void seeHeaderGroupSchema(String text) {
        assert "akiban_objects".equals(text): "saw groupschema" + text;
    }

    public GroupsBuilder getGroupsBuilder() {
        if (groupsBuilder == null) {
            throw new NullPointerException("haven't initialized groups builder yet");
        }
        return groupsBuilder;
    }
}
