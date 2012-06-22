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

import com.akiban.server.types3.CastContext;
import com.akiban.server.types3.TExecutionContext;

public final class CastUtils
{
     
    static long getInRange (long max, long min, long val, CastContext castContext, TExecutionContext excContext)
    {
        if (val > max)
        {
            castContext.reportError("Truncated " + val + " to " + max, excContext);
            return max;
        }
        else if (val < min)
        {
            castContext.reportError("Truncated " + val + " to " + min, excContext);
            return min;
        }
        else
            return val;
    }
    
     
    /**
     * Truncate non-digits part
     * @param st
     * @return 
     */
    static String truncateNonDigits(String st)
    {
        int n = 0;
        st = st.trim();
        
        // if the string starts with non-digits, return 0
        if (st.isEmpty() || !Character.isDigit(st.charAt(n++)) 
                && (n == st.length() || n < st.length() && !Character.isDigit(st.charAt(n++))))
            return "0";
        
        char ch;
        boolean sawDot = false;
        for (; n < st.length(); ++n)
            if ((!Character.isDigit(ch = st.charAt(n)))
                    && (sawDot || !(sawDot = ch == '.')))
                return st.substring(0, n);
        return st;
    }
}
