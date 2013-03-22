
package com.akiban.sql.pg;

public class ObjectLongPair {
    public final Object obj;
    public final long longVal;

    public ObjectLongPair(Object obj, long longVal) {
        assert obj != null : "Null obj for longVal " + longVal;
        this.obj = obj;
        this.longVal = longVal;
    }

    @Override
    public String toString() {
        return "[" + obj + "," + longVal + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ObjectLongPair rhs = (ObjectLongPair)o;
        return (longVal == rhs.longVal) && obj.equals(rhs.obj);
    }

    @Override
    public int hashCode() {
        int result = obj.hashCode();
        result = 31 * result + (int)(longVal ^ (longVal >>> 32));
        return result;
    }
}
