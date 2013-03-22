
package com.akiban.ais.model.aisb2;

public interface NewAISGroupIndexStarter {

    /**
     * Invokes {@link #on(String, String, String)} with the default schema
     * @param table the table name
     * @param column  the schema name
     * @return this builder instance
     */
    NewAISGroupIndexBuilder on(String table, String column);

    /**
     * Builds the first column of a group index.
     * This method sets the group for the upcoming group index; all subsequent calls to
     * {@link NewAISGroupIndexBuilder#and(String, String, String)} must reference tables also in this group.
     * @param schema the table's schema
     * @param table the table's name
     * @param column the column name
     * @return this builder instance
     */
    NewAISGroupIndexBuilder on(String schema, String table, String column);
}
