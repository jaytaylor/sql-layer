/* <GENERIC-HEADER - BEGIN>
 *
 * $(COMPANY) $(COPYRIGHT)
 *
 * Created on: Nov, 20, 2009
 * Created by: Thomas Hazel
 *
 * </GENERIC-HEADER - END> */

package com.akiban.ais.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Group implements Serializable, ModelNames
{
    public static Group create(AkibaInformationSchema ais, Map<String, Object> map)
    {
        return create(ais, (String) map.get(group_name));
    }

    public static Group create(AkibaInformationSchema ais, String groupName)
    {
        Group group = new Group(groupName);
        ais.addGroup(group);
        return group;
    }

    public Map<String, Object> map()
    {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(group_name, name);
        return map;
    }

    @SuppressWarnings("unused")
    private Group()
    {
        // GWT requires empty constructor
    }

    public Group(final String name)
    {
        this.name = name;
    }

    @Override
    public String toString()
    {
        return "Group(" + name + " -> " + groupTable.getName() + ")";
    }

    public String getName()
    {
        return name;
    }

    public String getDescription()
    {
        return name;
    }

    public GroupTable getGroupTable()
    {
        return groupTable;
    }

    public void setGroupTable(GroupTable groupTable)
    {
        this.groupTable = groupTable;
    }

    // State

    private String name;
    private GroupTable groupTable;
}
