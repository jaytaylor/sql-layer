
package com.akiban.server.error;

public class ColumnNotBoundException extends AkibanInternalException {
    public ColumnNotBoundException (String columnName, String location) {
        super (String.format("AIS User column `%s` not bound to DML at %s during compile.", columnName, location));
    }
}
