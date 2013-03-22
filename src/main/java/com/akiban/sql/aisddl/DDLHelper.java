
package com.akiban.sql.aisddl;

import com.akiban.ais.model.TableName;

public class DDLHelper {
    private DDLHelper() {}

    public static TableName convertName(String defaultSchema, com.akiban.sql.parser.TableName parserName) {
        final String schema = parserName.hasSchema() ? parserName.getSchemaName() : defaultSchema;
        return new TableName(schema, parserName.getTableName());
    }
}
