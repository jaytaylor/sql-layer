
package com.akiban.ais.util;

public class TableChange {
    public static enum ChangeType { ADD, DROP, MODIFY }

    private final String oldName;
    private final String newName;
    private final ChangeType changeType;

    private TableChange(String oldName, String newName, ChangeType changeType) {
        this.oldName = oldName;
        this.newName = newName;
        this.changeType = changeType;
    }

    public String getOldName() {
        return oldName;
    }

    public String getNewName() {
        return newName;
    }

    public ChangeType getChangeType() {
        return changeType;
    }

    @Override
    public String toString() {
        if(oldName != null && newName == null)
            return changeType + ":" + oldName;
        if(oldName == null && newName != null)
            return changeType + ":" + newName;
        return changeType + ":" + oldName + "->" + newName;
    }

    public static TableChange createAdd(String name) {
        return new TableChange(null, name, ChangeType.ADD);
    }

    public static TableChange createDrop(String name) {
        return new TableChange(name, null, ChangeType.DROP);
    }

    public static TableChange createModify(String oldName, String newName) {
        return new TableChange(oldName, newName, ChangeType.MODIFY);
    }
}
