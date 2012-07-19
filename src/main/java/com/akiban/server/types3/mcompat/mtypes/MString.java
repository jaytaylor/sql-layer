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

package com.akiban.server.types3.mcompat.mtypes;

import com.akiban.server.collation.AkCollatorFactory;
import com.akiban.server.error.StringTruncationException;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.common.types.StringAttribute;
import com.akiban.server.types3.common.types.StringFactory;
import com.akiban.server.types3.common.types.TString;
import com.akiban.server.types3.mcompat.MBundle;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.sql.types.TypeId;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

public class MString extends TString
{
    public static final MString CHAR = new MString(TypeId.CHAR_ID, "varchar");
    public static final MString VARCHAR = new MString(TypeId.VARCHAR_ID, "varchar");
    public static final MString TINYTEXT = new MString(TypeId.LONGVARCHAR_ID, "tinytext", 256);
    public static final MString MEDIUMTEXT = new MString(TypeId.LONGVARCHAR_ID, "mediumtext", 65535);
    public static final MString TEXT = new MString(TypeId.LONGVARCHAR_ID, "text", 16777215);
    public static final MString LONGTEXT = new MString(TypeId.LONGVARCHAR_ID, "longtext", Integer.MAX_VALUE); // TODO not big enough!

    @Override
    public void writeCollating(PValueSource inValue, TInstance inInstance, PValueTarget out) {
        final int collatorId = inInstance.attribute(StringAttribute.COLLATION);
        out.putString(inValue.getString(), AkCollatorFactory.getAkCollator(collatorId));
    }

    private MString(TypeId typeId, String name, int fixedSize) {
        super(typeId, MBundle.INSTANCE, name, -1, fixedSize);
    }
    
    private MString(TypeId typeId, String name)
    {       
        super(typeId, MBundle.INSTANCE, name, -1);
    }

    @Override
    public void fromObject(TExecutionContext context, PValueSource in, PValueTarget out)
    {
        int expectedLen = context.outputTInstance().attribute(StringAttribute.LENGTH);
        int charsetId = context.outputTInstance().attribute(StringAttribute.CHARSET);
        int collatorId = context.outputTInstance().attribute(StringAttribute.COLLATION);

        switch (in.getUnderlyingType())
        {
            case STRING:
                String inStr = in.getString();
                String ret;
                if (expectedLen > inStr.length())
                    ret = inStr;
                else
                {
                    ret = inStr.substring(0, expectedLen);
                    context.reportTruncate(inStr, ret);
                }
                out.putString(ret, AkCollatorFactory.getAkCollator(collatorId));
                break;
                
            case BYTES:
                byte bytes[] = in.getBytes();
                byte truncated[];

                if (bytes.length > expectedLen)
                {
                    context.reportTruncate("BYTES string of length " + bytes.length,
                                           "BYTES string of length " + expectedLen);
                    truncated = Arrays.copyOf(bytes, expectedLen);
                }
                else
                    truncated = bytes;
                
                try 
                {
                     out.putString(new String(truncated,
                                              StringFactory.Charset.of(charsetId))
                                  , AkCollatorFactory.getAkCollator(collatorId));
                }
                catch (UnsupportedEncodingException e)
                {
                    context.reportBadValue(e.getMessage());
                }
                break;
            default:
                throw new IllegalArgumentException("Unexpected UnderlyingType: " + in.getUnderlyingType());
        }
    }           
}
