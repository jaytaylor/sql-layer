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

import com.akiban.server.types3.*;
import com.akiban.server.types3.common.types.StringAttribute;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.texpressions.TOverloadBase;
import java.util.List;

public abstract class MSubstring extends TOverloadBase {

    protected int adjustIndex(String str, int index) {
        // String operand
        if (str.equals("")) {
            return -1;
        }
        
        // if from is negative or zero, start from the end, and adjust
        // index by 1 since index in sql starts at 1 NOT 0
        index += (index < 0? str.length() : -1);
       
        // if from is still neg, return -1
        if (index < 0) {
            return -1;
        }
        
        return index;
    }
    
    protected String getSubstring(int to, int from, String str) {            
        // if to <= fr => return empty
        if (to < from || from >= str.length())
        {
            return "";    
        }
        
        to = (to > str.length() - 1 ? str.length() - 1 : to); 
        return str.substring(from, to + 1);
    }

    @Override
    public String overloadName() {
        return "SUBSTRING";
    }
    
    public int calculateCharLength(int stringLength, int offset, int substringLength) {
        if (stringLength < Math.abs(offset) || substringLength <= 0)
            return 0;
        int ret = offset > 0 ? stringLength - offset + 1 : offset * -1;
        return Math.min(ret, substringLength);
    }
}
