
package com.akiban.ais.model.aisb2;

public interface NewAISGroupIndexBuilder extends NewAISProvider {

    /**
     * Invokes {@link #and(String, String, String)} with the default schema
     * @param table the table's name
     * @param column the column name
     * @return this builder instance
     */
    NewAISGroupIndexBuilder and(String table, String column);

    /**
     * Adds a column to a group index started by {@link NewAISGroupIndexStarter#on(String, String, String)}.
     * @param schema the schema
     * @param table the table's name
     * @param column the column name
     * @return this builder instance
     */
    NewAISGroupIndexBuilder and(String schema, String table, String column);
}
