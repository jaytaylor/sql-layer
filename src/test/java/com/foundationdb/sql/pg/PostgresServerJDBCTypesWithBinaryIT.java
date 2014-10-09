package com.foundationdb.sql.pg;

public class PostgresServerJDBCTypesWithBinaryIT extends PostgresServerJDBCTypesITBase {


    public PostgresServerJDBCTypesWithBinaryIT(String caseName, int jdbcType, String colName, Object value, String unparseable, Object defaultValue) {
        super(caseName, jdbcType, colName, value, unparseable, defaultValue);
    }

    @Override
    protected boolean binaryTransfer() {
        return true;
    }
}
