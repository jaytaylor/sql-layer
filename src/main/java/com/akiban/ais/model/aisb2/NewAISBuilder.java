package com.akiban.ais.model.aisb2;

import com.akiban.ais.model.AkibanInformationSchema;

public interface NewAISBuilder {
    /**
     * Gets the AIS that's been built.
     * @return the AIS
     */
    AkibanInformationSchema ais();

    /**
     * Sets the default schema
     * @param schema the new default schema name; like SQL's {@code USING}.
     * @return {@code this}
     */
    NewAISBuilder defaultSchema(String schema);

    /**
     * Starts creating a new table using the default schema.
     * @param table the table's name
     * @return the new table's builder
     */
    NewUserTableBuilder userTable(String table);

    /**
     * Starts creating a new table using the given schema
     * @param schema the new table's schema
     * @param table the new table's table name
     * @return the new table's builder
     */
    NewUserTableBuilder userTable(String schema, String table);
}
