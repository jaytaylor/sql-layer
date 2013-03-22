
package com.akiban.ais.model;

import com.akiban.ais.util.TableChange;

import java.util.List;

/** Attached to a <code>UserTable</code> on which <code>ALTER</code> has been performed
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
