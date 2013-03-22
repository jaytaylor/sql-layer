
package com.akiban.server.types3.mcompat.mcasts;

import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.TCastBase;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.texpressions.Constantness;

import static com.akiban.server.types3.mcompat.mcasts.MNumericCastBase.*;

public class Cast_From_Tinyint
{
    /**
     * TODO:
     * 
     * DATE
     * DATETIME
     * TIME
     * TIMESTAMP
     * YEAR
     * 
     * BIT
     * CHAR
     * BINARY
     * VARBINARY
     * TINYBLOG
     * TINYTEXT
     * TEXT
     * MEDIUMBLOB
     * MEDIUMTEXT
     * LONGBLOG
     * LONTTEXT
     * 
     */
    public static final TCast TO_UNSIGNED_TINYINT = new FromInt8ToUnsignedInt8(MNumeric.TINYINT, MNumeric.TINYINT_UNSIGNED, true, Constantness.UNKNOWN);
   
    public static final TCast TO_SMALLINT = new FromInt8ToInt16(MNumeric.TINYINT, MNumeric.SMALLINT, true, Constantness.UNKNOWN);

    public static final TCast TO_UNSIGNED_SMALLINT = new FromInt8ToUnsignedInt16(MNumeric.TINYINT, MNumeric.SMALLINT_UNSIGNED, true, Constantness.UNKNOWN);

    public static final TCast TO_MEDIUMINT = new FromInt8ToInt32(MNumeric.TINYINT, MNumeric.MEDIUMINT, true, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_MEDIUMINT = new FromInt8ToUnsignedInt32(MNumeric.TINYINT, MNumeric.MEDIUMINT_UNSIGNED, true, Constantness.UNKNOWN);

    public static final TCast TO_INT = new FromInt8ToInt32(MNumeric.TINYINT, MNumeric.INT, true, Constantness.UNKNOWN);

    public static final TCast TO_UNSIGNED_INT = new FromInt8ToUnsignedInt32(MNumeric.TINYINT, MNumeric.INT_UNSIGNED, true, Constantness.UNKNOWN);

    public static final TCast TO_BIGINT = new FromInt8ToInt64(MNumeric.TINYINT, MNumeric.BIGINT, true, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_BIGINT = new FromInt8ToInt64(MNumeric.TINYINT, MNumeric.BIGINT_UNSIGNED, true, Constantness.UNKNOWN);
    
    public static final TCast TO_DECIMAL = new FromInt8ToDecimal(MNumeric.TINYINT, MNumeric.DECIMAL, false, Constantness.UNKNOWN);
    
    public static final TCast TO_DOUBLE = new FromInt8ToDouble(MNumeric.TINYINT, MApproximateNumber.DOUBLE, true, Constantness.UNKNOWN);
}