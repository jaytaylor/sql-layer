
package com.akiban.server.types3.mcompat.mcasts;

import com.akiban.server.types3.TCast;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.texpressions.Constantness;

import static com.akiban.server.types3.mcompat.mcasts.MNumericCastBase.*;

public class Cast_From_Unsigned_Tinyint
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
    
    public static final TCast TO_TINYINT = new FromInt16ToInt8(MNumeric.TINYINT_UNSIGNED, MNumeric.TINYINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_SMALLINT = new FromInt16ToInt16(MNumeric.TINYINT_UNSIGNED, MNumeric.SMALLINT, false, Constantness.UNKNOWN);
 
    public static final TCast TO_UNSGINED_SMALLINT = new FromInt16ToInt32(MNumeric.TINYINT_UNSIGNED, MNumeric.SMALLINT_UNSIGNED, true, Constantness.UNKNOWN);

    public static final TCast TO_MEDIUMINT = new FromInt16ToInt32(MNumeric.TINYINT_UNSIGNED, MNumeric.MEDIUMINT, false, Constantness.UNKNOWN);
 
    public static final TCast TO_UNSIGNED_MEDIUMINT = new FromInt16ToInt64(MNumeric.TINYINT_UNSIGNED, MNumeric.MEDIUMINT_UNSIGNED, true, Constantness.UNKNOWN);
    
    public static final TCast TO_INT = new FromInt16ToInt32(MNumeric.TINYINT_UNSIGNED, MNumeric.INT, false, Constantness.UNKNOWN);
   
    public static final TCast TO_UNSIGNED_INT = new FromInt16ToInt64(MNumeric.TINYINT_UNSIGNED, MNumeric.INT_UNSIGNED, true, Constantness.UNKNOWN);
    
    public static final TCast TO_DECIMAL = new FromInt16ToDecimal(MNumeric.TINYINT_UNSIGNED, MNumeric.DECIMAL, false, Constantness.UNKNOWN);
    
    public static final TCast TO_DOUGLE = new FromInt16ToDouble(MNumeric.TINYINT_UNSIGNED, MApproximateNumber.DOUBLE, false, Constantness.UNKNOWN);
}
