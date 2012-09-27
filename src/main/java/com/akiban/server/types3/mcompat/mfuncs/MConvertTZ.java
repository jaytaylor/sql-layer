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

package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import static com.akiban.server.types3.mcompat.mtypes.MDatetimes.*;

public class MConvertTZ extends TOverloadBase
{
    public static final TOverload INSTANCE = new MConvertTZ();

    private MConvertTZ() {}
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(MDatetimes.DATETIME, 0).covers(MString.VARCHAR, 1, 2);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        long ymd[] = MDatetimes.decodeDatetime(inputs.get(0).getInt64());
        
        if (ymd[MDatetimes.YEAR_INDEX] == 0 
                || ymd[MDatetimes.MONTH_INDEX] == 0
                || ymd[MDatetimes.DAY_INDEX] == 0)
            output.putNull();
        else
        {
            DateTimeZone fromTz;
            DateTimeZone toTz;
            try {
                fromTz = adjustTz(inputs.get(1).getString());
                toTz = adjustTz(inputs.get(2).getString());
            }
            catch (IllegalArgumentException e) {
                output.putNull();
                return;
            }
            DateTime date = new DateTime((int) ymd[YEAR_INDEX],
                                         (int) ymd[MONTH_INDEX],
                                         (int) ymd[DAY_INDEX],
                                         (int) ymd[HOUR_INDEX],
                                         (int) ymd[MIN_INDEX],
                                         (int) ymd[SEC_INDEX],
                                         0, // a DATETIME is only accurate up to SECOND,
                                         fromTz); // thus the MILLIS SEC field is 0
            
            output.putInt64(encodeDatetime(date.withZone(toTz)));
        }
    }

    @Override
    public String displayName()
    {
        return "CONVERT_TZ";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(MDatetimes.DATETIME.instance());
    }
    
    /**
     * joda's non-named timezone  (off-set hour) is in the form:
     * [<PLUS>] | <MINUS>]<NUMBER><NUMBER><COLON><NUMBER><NUMBER>
     * 
     * 1 digit number is not use.
     * 
     * - This prepend 0 to make it 2 digit number
     * - Named timezones are also 'normalised' 
     * @param st
     * @return 
     */
    private static DateTimeZone adjustTz(String st)
    {
        for ( int n = 0; n < st.length(); ++n)
        {
            char ch;
            if ((ch = st.charAt(n)) == ':')
            {
                int index = n - 2; // if the character that is 2 chars to the left of the COLON
                if (index < 0 )    //  is not a digit, then we need to pad a '0' there
                    return DateTimeZone.forID(st);
                ch = st.charAt(index);
                if (ch == '-' || ch == '+')
                {
                    StringBuilder bd = new StringBuilder(st);
                    bd.insert(1, '0');
                    return DateTimeZone.forID(bd.toString());
                }
                break;
            }
            else if (ch == '/')
                return DateTimeZone.forID(st);
        }
        return DateTimeZone.forID(st.toUpperCase());
    }
}
