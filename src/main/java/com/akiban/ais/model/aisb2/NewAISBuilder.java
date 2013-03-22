
package com.akiban.ais.model.aisb2;

import com.akiban.ais.model.TableName;

public interface NewAISBuilder extends NewAISProvider {
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

    NewUserTableBuilder userTable(TableName tableName);
    
    /**
     * Returns the NewUserTableBuilder for the table being built 
     * @return
     */
    NewUserTableBuilder getUserTable();
    NewUserTableBuilder getUserTable(TableName table);
   
    /**
     * create a new sequence
     */
    NewAISBuilder sequence (String name);
    NewAISBuilder sequence (String name, long start, long increment, boolean isCycle);

    /**
     * create a new view 
     * @param view name of view
     * @return
     */
    NewViewBuilder view(String view);

    NewViewBuilder view(String schema, String view);

    NewViewBuilder view(TableName viewName);

    /**
     * create a new procedure 
     * @param procedure name of procedure
     * @return
     */
    NewRoutineBuilder procedure(String procedure);

    NewRoutineBuilder procedure(String schema, String procedure);

    NewRoutineBuilder procedure(TableName procedureName);

    /**
     * create a new SQL/J jar 
     * @param jarName name of jar file
     * @return
     */
    NewSQLJJarBuilder sqljJar(String jarName);

    NewSQLJJarBuilder sqljJar(String schema, String jarName);

    NewSQLJJarBuilder sqljJar(TableName name);
}
