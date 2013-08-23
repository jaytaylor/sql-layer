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

package com.foundationdb.server.expression.std;

import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.error.WrongExpressionArityException;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.expression.ExpressionType;
import com.foundationdb.server.expression.TypesList;
import com.foundationdb.server.service.functions.Scalar;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.NullValueSource;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.conversion.Converters;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.types.CharacterTypeAttributes;
import com.foundationdb.util.ByteSource;
import java.io.UnsupportedEncodingException;
import java.util.zip.CRC32;

public class CRC32Expression extends AbstractUnaryExpression
{
    @Scalar("crc32")
    public static final ExpressionComposer COMPOSER = new UnaryComposer()
    {
        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType)
        {
            String charset;
            CharacterTypeAttributes att;

            if (argType != null
                && (att = argType.getCharacterAttributes()) != null)
                charset = att.getCharacterSet();
            else
                charset = Converters.DEFAULT_CS;

            return new CRC32Expression(argument, charset);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1)
                throw new WrongExpressionArityException(1, argumentTypes.size());
    
            // don't cast the type to VARCHAR if it's alraedy a VARBINARY
            if (argumentTypes.get(0).getType() != AkType.VARBINARY)
                argumentTypes.setType(0, AkType.VARCHAR);

            return ExpressionTypes.LONG;
        }
    };

    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private final String charset;

        InnerEvaluation(ExpressionEvaluation arg, String charset)
        {
            super(arg);
            this.charset = charset;
        }

        @Override
        public ValueSource eval()
        {
            try
            {
                ValueSource arg = operand();
                if (arg.isNull())
                    return NullValueSource.only();
                
                CRC32 crc32 = new CRC32();
                
                if (arg.getConversionType() == AkType.VARCHAR)
                {
                    byte byteArr[] = arg.getString().getBytes(charset);
                    crc32.update(byteArr);
                }
                else
                {
                    ByteSource varbin = arg.getVarBinary();
                    crc32.update(varbin.byteArray(),
                                 varbin.byteArrayOffset(),
                                 varbin.byteArrayLength());
                }

                valueHolder().putLong(crc32.getValue());
                return valueHolder();
            }
            catch (UnsupportedEncodingException ex)
            {
                QueryContext qc = queryContext();
                if (qc != null)
                    qc.warnClient(new InvalidParameterValueException("Invalid charset: " + charset));
                return NullValueSource.only();
            }
        }
    }
    
    private final String charset;
    
    protected CRC32Expression(Expression argument, String charset)
    {
        super(AkType.LONG, argument);
        this.charset = charset;
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(operandEvaluation(), charset);
    }

    @Override
    public String name()
    {
        return "CRC32";
    }
}
