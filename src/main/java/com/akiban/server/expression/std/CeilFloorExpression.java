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

package com.akiban.server.expression.std;

import com.akiban.qp.operator.QueryContext;
import com.akiban.server.error.InvalidArgumentTypeException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.*;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class CeilFloorExpression extends AbstractUnaryExpression {
    @Scalar("floor")
    public static final ExpressionComposer FLOOR_COMPOSER = new InternalComposer(CeilFloorName.FLOOR);
    
    @Scalar({"ceil", "ceiling"})
    public static final ExpressionComposer CEIL_COMPOSER = new InternalComposer(CeilFloorName.CEIL);

    public static enum CeilFloorName
    {
        FLOOR, CEIL;
    }
    
    private final CeilFloorName name;

    private static class InternalComposer extends UnaryComposer
    {
        private final CeilFloorName name;

        public InternalComposer(CeilFloorName funcName)
        {
            this.name = funcName;
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            int argc = argumentTypes.size();
            if (argc != 1)
                throw new WrongExpressionArityException(1, argc);
            
            AkType firstAkType = argumentTypes.get(0).getType();

            if (firstAkType == AkType.VARCHAR || firstAkType == AkType.UNSUPPORTED)
            {
                argumentTypes.setType(0, AkType.DOUBLE);
            }
                        
            return argumentTypes.get(0);
        }

        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType)
        {
            return new CeilFloorExpression(argument, name);
        }
    }

    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {

        private final CeilFloorName name;

        public InnerEvaluation(ExpressionEvaluation eval, CeilFloorName funcName)
        {
            super(eval);
            this.name = funcName;
        }

        @Override
        public ValueSource eval()
        {
            ValueSource firstOperand = operand();
            if (firstOperand.isNull())
                return NullValueSource.only();

            AkType operandType = firstOperand.getConversionType();

            switch (operandType)
            {
                // For any integer type, ROUND/FLOOR/CEIL just return the same value
                case INT: 
                case LONG: 
                case U_INT: 
                case U_BIGINT:
                    valueHolder().copyFrom(firstOperand); 
                    break;
                // Math.floor/ceil only with doubles, so we split FLOAT/DOUBLE to be safe with casting
                case DOUBLE:
                    double dInput = firstOperand.getDouble();
                    double finalDValue = (name == CeilFloorName.FLOOR) ? Math.floor(dInput) : Math.ceil(dInput);
                    valueHolder().putDouble(finalDValue); 
                    break;
                case U_DOUBLE:
                    double unsignedDInput = firstOperand.getUDouble();
                    double finalUnsignedDValue = (name == CeilFloorName.FLOOR) ? Math.floor(unsignedDInput) : Math.ceil(unsignedDInput);
                    valueHolder().putUDouble(finalUnsignedDValue); 
                    break;                    
                case FLOAT:
                    float fInput = firstOperand.getFloat();
                    float finalFValue = (float) ((name == CeilFloorName.FLOOR) ? Math.floor(fInput) : Math.ceil(fInput));
                    valueHolder().putFloat(finalFValue);
                    break;
                case U_FLOAT:
                    float unsignedFInput = firstOperand.getUFloat();
                    float finalUnsignedFValue = (float) ((name == CeilFloorName.FLOOR) ? Math.floor(unsignedFInput) : Math.ceil(unsignedFInput));
                    valueHolder().putUFloat(finalUnsignedFValue);
                    break;
                case DECIMAL:
                    // NOTE: BigDecimal equality requires .compareTo(); using .equals() checks if scale is equal as well
                    // In IT, this returns an integral value since its scale is zero.
                    
                    BigDecimal decInput = firstOperand.getDecimal();
                    BigDecimal finalDecValue = (name == CeilFloorName.FLOOR) ? 
                            decInput.setScale(0, RoundingMode.FLOOR) : 
                            decInput.setScale(0, RoundingMode.CEILING);
                    valueHolder().putDecimal(finalDecValue);
                    break;
                default:
                    QueryContext context = queryContext();
                    if (context != null)
                        context.warnClient(new InvalidArgumentTypeException(name.name() + operandType.name()));
                    return NullValueSource.only();
            }
            return valueHolder();
        }
    }
    
    protected CeilFloorExpression(Expression operand, CeilFloorName funcName)
    {
        super(operand.valueType(), operand);
        this.name = funcName;
    }
    
    @Override
    public String name()
    {
        return name.name();
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(this.operandEvaluation(), name);
    }
}
