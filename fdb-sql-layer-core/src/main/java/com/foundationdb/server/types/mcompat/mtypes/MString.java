/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.types.mcompat.mtypes;

import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.collation.AkCollatorFactory;
import com.foundationdb.server.types.ValueIO;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.StringFactory;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.mcompat.MBundle;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.sql.types.TypeId;
import com.foundationdb.util.Strings;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

public class MString extends TString
{

    public static TInstance varcharFor(String string) {
        return string == null
                ? MString.VARCHAR.instance(0, true)
                : MString.VARCHAR.instance(string.length(), false);
    }
    
    public static TInstance varchar() {
        return MString.VARCHAR.instance(true);
    }

    public static final MString CHAR = new MString(TypeId.CHAR_ID, "char");
    public static final MString VARCHAR = new MString(TypeId.VARCHAR_ID, "varchar");
    public static final MString TINYTEXT = new MString(TypeId.TINYTEXT_ID, "tinytext", 255);
    public static final MString TEXT = new MString(TypeId.TEXT_ID, "text", 65535);
    public static final MString MEDIUMTEXT = new MString(TypeId.MEDIUMTEXT_ID, "mediumtext", 16777215);
    public static final MString LONGTEXT = new MString(TypeId.LONGTEXT_ID, "longtext", Integer.MAX_VALUE); // TODO not big enough!

    @Override
    protected ValueIO getValueIO() {
        return PVALUE_IO;
    }

    @Override
    public void selfCast(TExecutionContext context, TInstance sourceInstance, ValueSource source,
                         TInstance targetInstance, ValueTarget target) {
        int maxTargetLen = targetInstance.attribute(StringAttribute.MAX_LENGTH);
        String sourceString = source.getString();
        String truncated = Strings.truncateIfNecessary(sourceString, maxTargetLen);
        if (sourceString != truncated) {
            context.reportTruncate(sourceString, truncated);
            sourceString = truncated;
        }
        target.putString(sourceString, null);
    }

    private MString(TypeId typeId, String name, int fixedSize) {
        super(typeId, MBundle.INSTANCE, name, -1, fixedSize);
    }
    
    private MString(TypeId typeId, String name)
    {       
        super(typeId, MBundle.INSTANCE, name, -1);
    }

    @Override
    public boolean compatibleForCompare(TClass other) {
        return super.compatibleForCompare(other) ||
            ((this == CHAR) && (other == VARCHAR)) ||
            ((this == VARCHAR) && (other == CHAR));
    }

    // Generally, text fields are rarely used in comparisons other than 'EQUAL'
    // Hence, it is not necessary to widen the width to capture the widest type
    // If the input does not fit into *this* width, it is definitely not going to be equal
    // Might need to flag 'TRUNCATION' as an error, not warning
    public TClass widestComparable()
    {
        return this;
    }
    
    @Override
    public void fromObject(TExecutionContext context, ValueSource in, ValueTarget out)
    {
        if (in.isNull()) {
            out.putNull();
            return;
        }
        int expectedLen = context.outputType().attribute(StringAttribute.MAX_LENGTH);
        int charsetId = context.outputType().attribute(StringAttribute.CHARSET);
        int collatorId = context.outputType().attribute(StringAttribute.COLLATION);

        switch (TInstance.underlyingType(in.getType()))
        {
            case STRING:
                String inStr = in.getString();
                String ret = Strings.truncateIfNecessary(inStr, expectedLen);
                if (inStr != ret)
                {
                    context.reportTruncate(inStr, ret);
                }
                out.putString(ret, AkCollatorFactory.getAkCollator(collatorId));
                break;
                
            case BYTES:
                byte bytes[] = in.getBytes();
                byte truncated[];

                if (bytes.length > expectedLen)
                {
                    truncated = Arrays.copyOf(bytes, expectedLen);
                    context.reportTruncate("BYTES string of length " + bytes.length,
                                           "BYTES string of length " + expectedLen);
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
                throw new IllegalArgumentException("Unexpected UnderlyingType: " + in.getType());
        }
    }

    private static final ValueIO PVALUE_IO = new ValueIO() {
        @Override
        public void copyCanonical(ValueSource in, TInstance typeInstance, ValueTarget out) {
            out.putString(in.getString(), null);
        }

        @Override
        public void writeCollating(ValueSource inValue, TInstance inInstance, ValueTarget out) {
            final AkCollator collator = getCollator(inInstance);
            if (inValue.getObject() instanceof byte[]) {
                out.putBytes((byte[])inValue.getObject());
            }
            else if (inValue.getObject() instanceof String || inValue.getObject() == null) {
                out.putString((String)inValue.getObject(), collator);
            }
            else {
                throw new UnsupportedOperationException("Unexpected inValue: " + inValue.getObject());
            }
        }

        @Override
        public void readCollating(ValueSource in, TInstance typeInstance, ValueTarget out) {
            if (in.canGetRawValue())
                out.putString(in.getString(), null);
            else if (in.hasCacheValue())
                out.putObject(in.getObject());
            else
                throw new AssertionError("no value");
        }
    };
}
