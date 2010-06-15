/**
 * 
 */
package com.akiban.cserver.store;

import java.util.BitSet;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.persistit.KeyState;

/**
 * @author percent
 * 
 */
public class Delta implements Comparable {
    public enum Type {Insert, Update, Delete};

    public Delta(Type type, KeyState key, RowDef rowDef, RowData rowData) {
        assert type == Type.Insert;
        this.type = type;
        this.key = key;
        this.rowDef = rowDef;
        this.rowData = rowData;
        this.nullMap = new BitSet(rowDef.getFieldCount());

        for (int i = 0; i < rowDef.getFieldCount(); i++) {
            if (this.rowData.isNull(i)) {
                //assert false;
                this.nullMap.set(i, true);
            }
        }
    }

    public boolean isProjection() {
        return (nullMap.isEmpty() ? false : true);
    }

    public BitSet getNullMap() {
        return nullMap;
    }
    
    public Type getType() {
        return type;
    }

    public RowDef getRowDef() {
        return rowDef;
    }

    public RowData getRowData() {
        return rowData;
    }

    public KeyState getKey() {
        return key;
    }

    public int compareTo(Object obj) {
        assert obj instanceof Delta;
        assert key != null;
        assert ((Delta)obj).key != null;
        return key.compareTo(((Delta) obj).key);
    }

    private final Type type;
    private final KeyState key;
    private final RowDef rowDef;
    private final RowData rowData;
    private final BitSet nullMap;
}
