
package com.akiban.sql.server;

import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;

/** A type according to the server's regime.
 */
public abstract class ServerType
{
    public enum BinaryEncoding {
        NONE, INT_8, INT_16, INT_32, INT_64, FLOAT_32, FLOAT_64, STRING_BYTES,
        BINARY_OCTAL_TEXT, BOOLEAN_C, 
        TIMESTAMP_FLOAT64_SECS_2000_NOTZ, TIMESTAMP_INT64_MICROS_2000_NOTZ,
        DECIMAL_PG_NUMERIC_VAR
    }

    private AkType akType;
    private TInstance instance;

    protected ServerType(AkType akType, TInstance instance) {
        this.akType = akType;
        this.instance = instance;
    }

    public AkType getAkType() {
        return akType;
    }
    
    public TInstance getInstance() {
        return instance;
    }

    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.NONE;
    }

    @Override
    public String toString() {
        return String.valueOf(akType);
    }

}
