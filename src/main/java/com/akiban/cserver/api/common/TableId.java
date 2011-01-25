package com.akiban.cserver.api.common;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.akiban.ais.model.TableName;
import com.akiban.util.CacheMap;

/**
 * <p>A wrapper for a table ID, identified either by the table's int ID or by the table's name.</p>
 *
 * <p>A TableId is a union of TableName and int ID; at least one of those must be defined for each TableId.
 * If both are known, the int always takes precedence. A TableId which has an int defined is considered
 * <strong>resolved</strong>, and a TableId which does not is known as <strong>unresolved</strong>. Once a TableID
 * has an int ID defined, that ID cannot be changed. Only resolved TableId instances can be used in a Set or
 * as the keys to a Map (see below).</p>
 *
 * <p>A TableId represents a table in the abstract, but there is no guarantee that the table represented actually
 * exists in the system; this is true whether the TableId is resolved.</p>
 *
 * <p>This class' equality and hash code depend on the int ID of the table it represents. This may not be known
 * at time of construction, if the TableId hasn't been resolved. As such, an unresolved TableId has an undefined
 * equality and hash code, and cannot be used in any contexts (such as a Set or a Map's key) which require
 * them. Any attempt to do so will result in a ResolutionException being thrown.</p>
 */
public class TableId {

    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
    private final static Map<Integer, TableId> cache = Collections.synchronizedMap(
            new CacheMap<Integer, TableId>(new CacheMap.Allocator<Integer, TableId>() {
                @Override
                public TableId allocateFor(Integer key) {
                    return new TableId(key);
                }
            })
    );
    
    private final AtomicReference<Integer> tableId = new AtomicReference<Integer>();
    private final TableName tableName;

    public static TableId of(int tableId) {
        return cache.get(tableId);
    }

    public static TableId of(String schemaName, String tableName) {
        return new TableId(schemaName, tableName);
    }

    private TableId(int tableId) {
        this(tableId, null);
    }

    private TableId(String schemaName, String tableName) {
        this(null, TableName.create(schemaName, tableName));
    }

    protected TableId(Integer id, TableName tableName) {
        this.tableId.set(id);
        this.tableName = tableName;
    }

    /**
     * <p>Whether this TableId has been resolved to an integer table ID. A TableId can be specified by name, by
     * integer or by both (if both are given, the integer takes precedence). A resolved TableId has properties a
     * non-resolved one doesn't have (see below).
     *
     * <p>A table is considered resolved if it has an integer value. This can happen a couple of
     * different ways:
     * <ul>
     *  <li>by constructing the TableId by int</li>
     *  <li>by invoking {@linkplain #getTableId(IdResolver)}</li>
     * <ul></p>
     *
     * <p>A TableId can only be resolved once; once the integer ID is known, it is always used in preference to
     * the TableName.</p>
     *
     * <p>A resolved TableId and unresolved TableId work in different ways:
     * <ul>
     *  <li>{@linkplain #getTableId(IdResolver)} may take a null IdResolver iff the TableId is resolved
     * (since resolution isn't needed in that case)</li>
     *  <li>equality and hash code computation only work with resolved TableIds</li>
     * </ul>
     * </p>
     *
     * <p>Note that resolution only caches the table's integer ID, not the TableName. This means that every invocation
     * of {@linkplain #getTableName(IdResolver)} will use the IdResolver to look up the table name. This is due to the
     * fact that a table's name may change, but its ID is, by definition, unique and persistent. One consequence of this
     * is that if you create a TableId by name and then store it, you could end up referring to a table you didn't mean
     * to. Consider a TableId("s", "c1") that refers to table ID 1. The user then deletes that table and creates a
     * new table with the same name; it'll have a different ID. Is you had already resolved the initial TableId, its
     * integer value will continue to point to the original, deleted table ID.</p>
     * @return whether this TableId is resolved
     */
    public boolean isResolved() {
        return tableId.get() != null;
    }

    public int getTableId(IdResolver resolver) throws NoSuchTableException, ResolutionException {
        Integer tableIdInteger = tableId.get();
        if (tableIdInteger != null) {
            return tableIdInteger;
        }

        int tableIdInt = resolver.tableId(tableName);
        if (!tableId.compareAndSet(null, tableIdInt)) {
            Integer old = tableId.get();
            if (old == null || (old != tableIdInt) ) {
                throw new ResolutionException(tableIdInteger, tableName, "tableIdInt="+tableIdInt);
            }
        }
        return tableIdInt;
    }

    public TableName getTableName(IdResolver resolver) throws NoSuchTableException {
        if (tableName != null) {
            return tableName;
        }
        return resolver.tableName(tableId.get());
    }

    @Override
    public boolean equals(Object o) {
        Integer myIdInt = tableId.get();
        if (myIdInt == null) {
            throw new ResolutionException(myIdInt, tableName);
        }

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableId tableId1 = (TableId) o;
        Integer otherIdInt = tableId1.tableId.get();
        if (otherIdInt == null) {
            throw new ResolutionException(myIdInt, tableName, "otherIdInt==null, otherTableName="+tableId1.tableName);
        }
        return myIdInt.equals(otherIdInt);
    }

    @Override
    public int hashCode() {
        Integer tableIdInt = tableId.get();
        if (tableIdInt == null) {
            throw new ResolutionException(tableIdInt, tableName);
        }
        return tableIdInt.hashCode();
    }
}