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

package com.akiban.server.types3;

import com.akiban.server.types3.common.types.StringAttribute;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.util.AkibanAppender;

public abstract class TClassBase extends TClass
{
    private final TParser parser;
    private final int defaultVarcharLen;
    
    protected <A extends Enum<A> & Attribute> TClassBase(TBundleID bundle,
            String name,
            Enum<?> category,
            Class<A> enumClass,
            TClassFormatter formatter,
            int internalRepVersion, int sVersion, int sSize,
            PUnderlying pUnderlying,
            TParser parser,
            int defaultVarcharLen)
     {
         super(bundle,
               name,
               category,
               enumClass,
               formatter,
               internalRepVersion,
               sVersion,
               sSize,
               pUnderlying);
         
         this.parser = parser;
         this.defaultVarcharLen = defaultVarcharLen;
     }
     
    @Override
     public void fromObject(TExecutionContext context, PValueSource in, PValueTarget out)
     {
         if (in.isNull()) {
             out.putNull();
         }
         else {
             PUnderlying underlyingType = in.getUnderlyingType();
             if (underlyingType != PUnderlying.STRING && underlyingType != PUnderlying.BYTES) {
                 // This isn't efficient, but it normalizes conversions of different inputs conveniently.
                 // This method isn't used in any tight loops, so some inefficiency is okay.
                 final String asString;
                 switch (underlyingType) {
                 case BOOL:
                     asString = Boolean.toString(in.getBoolean());
                     break;
                 case INT_8:
                     asString = Byte.toString(in.getInt8());
                     break;
                 case INT_16:
                     asString = Short.toString(in.getInt16());
                     break;
                 case UINT_16:
                     asString = Integer.toString(in.getUInt16());
                     break;
                 case INT_32:
                     asString = Integer.toString(in.getInt32());
                     break;
                 case INT_64:
                     asString = Long.toString(in.getInt64());
                     break;
                 case FLOAT:
                     asString = Float.toString(in.getFloat());
                     break;
                 case DOUBLE:
                     asString = Double.toString(in.getDouble());
                     break;
                 case BYTES:
                 case STRING:
                 default:
                     throw new AssertionError(underlyingType + ": " + in);
                 }
                 in = new PValue(MString.VARCHAR, asString);
             }
            parser.parse(context, in, out);
         }
     }

    @Override
    public TCast castToVarchar() {
        return new TCastBase(this, MString.VARCHAR) {
            @Override
            protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
                AkibanAppender appender = (AkibanAppender) context.exectimeObjectAt(APPENDER_CACHE_INDEX);
                StringBuilder sb;
                if (appender == null) {
                    sb = new StringBuilder();
                    appender = AkibanAppender.of(sb);
                    context.putExectimeObject(APPENDER_CACHE_INDEX, appender);
                }
                else {
                    sb = (StringBuilder) appender.getAppendable();
                    sb.setLength(0);
                }
                format(context.inputTInstanceAt(0), source, appender);
                String string = sb.toString();
                int maxlen = context.outputTInstance().attribute(StringAttribute.LENGTH);
                if (string.length() > maxlen) {
                    String trunc = sb.substring(0, maxlen);
                    context.reportTruncate(string, trunc);
                    string = trunc;
                }
                target.putString(string, null);
            }

            @Override
            public TInstance preferredTarget(TPreptimeValue source) {
                int len;
                if (source.value() == null) {
                    len = defaultVarcharLen;
                }
                else {
                    StringBuilder sb = new StringBuilder();
                    format(source.instance(), source.value(), AkibanAppender.of(sb));
                    len = sb.length();
                }
                return MString.VARCHAR.instance(len, source.isNullable());
            }
        };
    }

    @Override
    public TCast castFromVarchar() {
        return new TCastBase(MString.VARCHAR, this) {
            @Override
            protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
                parser.parse(context, source, target);
            }
        };
    }

    private static final int APPENDER_CACHE_INDEX = 0;
}
