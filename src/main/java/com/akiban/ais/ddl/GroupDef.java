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
        assert "akiba_objects".equals(text): "saw groupschema" + text;
    }

    public GroupsBuilder getGroupsBuilder() {
        if (groupsBuilder == null) {
            throw new NullPointerException("haven't initialized groups builder yet");
        }
        return groupsBuilder;
    }
}
