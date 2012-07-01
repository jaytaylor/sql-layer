/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.types3.mcompat.mcasts;

import com.akiban.server.types3.TCast;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
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
    public static final TCast TO_UNSIGNED_TINYINT = new FromInt8ToInt16(MNumeric.TINYINT, MNumeric.TINYINT_UNSIGNED, true, Constantness.UNKNOWN);
   
    public static final TCast TO_SMALLINT = new FromInt8ToInt16(MNumeric.TINYINT, MNumeric.SMALLINT, true, Constantness.UNKNOWN);

    public static final TCast TO_UNSIGNED_SMALLINT = new FromInt8ToInt32(MNumeric.TINYINT, MNumeric.SMALLINT_UNSIGNED, true, Constantness.UNKNOWN);

    public static final TCast TO_MEDIUMINT = new FromInt8ToInt32(MNumeric.TINYINT, MNumeric.MEDIUMINT, true, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_MEDIUMINT = new FromInt8ToInt64(MNumeric.TINYINT, MNumeric.MEDIUMINT_UNSIGNED, true, Constantness.UNKNOWN);

    public static final TCast TO_INT = new FromInt8ToInt32(MNumeric.TINYINT, MNumeric.INT, true, Constantness.UNKNOWN);

    public static final TCast TO_UNSIGNED_INT = new FromInt8ToInt64(MNumeric.TINYINT, MNumeric.INT_UNSIGNED, true, Constantness.UNKNOWN);

    public static final TCast TO_BIGINT = new FromInt8ToInt64(MNumeric.TINYINT, MNumeric.BIGINT, true, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_BIGINT = new FromInt8ToInt64(MNumeric.TINYINT, MNumeric.BIGINT_UNSIGNED, true, Constantness.UNKNOWN);
    
    public static final TCast TO_DECIMAL = new FromInt8ToDecimal(MNumeric.TINYINT, MNumeric.DECIMAL, true, Constantness.UNKNOWN);
    
    public static final TCast TO_DOUBLE = new FromInt8ToDouble(MNumeric.TINYINT, MApproximateNumber.DOUBLE, true, Constantness.UNKNOWN);
    
    public static final TCast TO_VARCHAR = new FromInt8ToString(MNumeric.TINYINT, MString.VARCHAR, true, Constantness.UNKNOWN);
}