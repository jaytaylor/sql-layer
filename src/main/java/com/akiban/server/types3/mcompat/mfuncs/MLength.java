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

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.common.types.StringAttribute;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.common.types.StringFactory;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;
import com.google.common.collect.ObjectArrays;

import java.io.UnsupportedEncodingException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * Implement the length (char_length and octet_length)
 */
public abstract class MLength extends TOverloadBase
{
    public static final TOverload CHAR_LENGTH = new MLength("CHAR_LENGTH")
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            output.putInt32((inputs.get(0).getString()).length());
        }

        @Override
        public String[] registeredNames() {
            return new String[] { "char_length", "charLength" };
        }
    };

    public static final TOverload OCTET_LENGTH = new MBinaryLength("OCTET_LENGTH", 1, "getOctetLength");
    public static final TOverload BIT_LENGTH = new MBinaryLength("BIT_LENGTH", 8);

    private static class MBinaryLength extends MLength
    {

        private final int multiplier;
        private final String[] aliases;

        private MBinaryLength(String name, int multiplier, String... aliases) {
            super(name);
            this.multiplier = multiplier;
            this.aliases = ObjectArrays.concat(aliases, name);
        }

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            int charsetId = context.inputTInstanceAt(0).attribute(StringAttribute.CHARSET);
            String charset = (StringFactory.Charset.values())[charsetId].name();
            try
            {
                int length = (inputs.get(0).getString()).getBytes(charset).length;
                length *= multiplier;
                output.putInt32(length);
            }
            catch (UnsupportedEncodingException ex) // impossible to happen
            {
                Logger.getLogger(MLength.class.getName()).log(Level.WARNING, null, ex);
                output.putNull();
            }
        }

        @Override
        public String[] registeredNames() {
            return aliases;
        }
    }

    private final String name;

    private MLength (String name)
    {
        this.name = name;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(MString.VARCHAR, 0);
    }



    @Override
    public String displayName()
    {
        return name;
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(MNumeric.INT.instance());
    }
}