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

import com.akiban.server.explain.*;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TCustomOverloadResult;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TInstanceGenerator;
import com.akiban.server.types3.TInstanceNormalizer;
import com.akiban.server.types3.TInstanceNormalizers;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.aksql.aktypes.AkInterval;
import com.akiban.server.types3.common.BigDecimalWrapper;
import com.akiban.server.types3.common.funcs.TArithmetic;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MBigDecimal.Attrs;
import com.akiban.server.types3.mcompat.mtypes.MBigDecimal;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.Constantness;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.server.types3.texpressions.TScalarBase;
import com.google.common.primitives.Doubles;

import java.util.List;

public abstract class MArithmetic extends TArithmetic {

    private static final int DEC_INDEX = 0;
    
    private final String infix;
    private final boolean associative;

    private MArithmetic(String overloadName, String infix, boolean associative, TClass operand0, TClass operand1,
                        TClass resultType, int... attrs)
    {
        super(overloadName, operand0, operand1, resultType, attrs);
        this.infix = infix;
        this.associative = associative;
    }

    private MArithmetic(String overloadName, String infix, boolean associative, TClass operand,
                        TClass resultType, int... attrs)
    {
        this(overloadName, infix, associative, operand, operand, resultType, attrs);
    }

    @Override
    protected String toStringName() {
        return "Arith";
    }

    @Override
    protected String toStringArgsPrefix() {
        return infix + " -> ";
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context, List<? extends TPreparedExpression> inputs, TInstance resultType)
    {
        CompoundExplainer ex = super.getExplainer(context, inputs, resultType);
        if (infix != null)
            ex.addAttribute(Label.INFIX_REPRESENTATION, PrimitiveExplainer.getInstance(infix));
        if (associative)
            ex.addAttribute(Label.ASSOCIATIVE, PrimitiveExplainer.getInstance(associative));
        return ex;
    }
    
    // Add functions
    public static final TScalar ADD_TINYINT = new MArithmetic("plus", "+", true, MNumeric.TINYINT, MNumeric.MEDIUMINT, 5) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int a0 = inputs.get(0).getInt8();
            int a1 = inputs.get(1).getInt8();
            output.putInt32(a0 + a1);
        }
    };

    public static final TScalar ADD_SMALLINT = new MArithmetic("plus", "+", true, MNumeric.SMALLINT, MNumeric.MEDIUMINT, 7) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int a0 = inputs.get(0).getInt16();
            int a1 = inputs.get(1).getInt16();
            output.putInt32(a0 + a1);
        }
    };

    public static final TScalar ADD_MEDIUMINT = new MArithmetic("plus", "+", true, MNumeric.MEDIUMINT, MNumeric.INT, 9) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int a0 = inputs.get(0).getInt32();
            int a1 = inputs.get(1).getInt32();
            output.putInt32(a0 + a1);
        }
    };

    public static final TScalar ADD_INT = new MArithmetic("plus", "+", true, MNumeric.INT, MNumeric.BIGINT, 12) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            long a0 = inputs.get(0).getInt32();
            long a1 = inputs.get(1).getInt32();
            output.putInt64(a0 + a1);
        }
    };

    public static final TScalar ADD_BIGINT = new MArithmetic("plus", "+", true, MNumeric.BIGINT, MNumeric.BIGINT, 21) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            long a0 = inputs.get(0).getInt64();
            long a1 = inputs.get(1).getInt64();
            output.putInt64(a0 + a1);
        }
    };

    public static final TScalar ADD_DECIMAL = new DecimalArithmetic("plus", "+", true) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            output.putObject(MBigDecimal.getWrapper(context, DEC_INDEX)
                        .set(MBigDecimal.getWrapper(inputs.get(0), context.inputTInstanceAt(1)))
                        .add(MBigDecimal.getWrapper(inputs.get(1), context.inputTInstanceAt(1))));
        }

        @Override
        protected long precisionAndScale(int arg0Precision, int arg0Scale, int arg1Precision, int arg1Scale) {
            return plusOrMinusArithmetic(arg0Precision, arg0Scale, arg1Precision, arg1Scale);
        }
    };

    public static final TScalar ADD_DOUBLE = new MArithmetic("plus", "+", true, MApproximateNumber.DOUBLE, MApproximateNumber.DOUBLE)
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            output.putDouble(inputs.get(0).getDouble() + inputs.get(1).getDouble());
        }
    };

    // Subtract functions
    public static final TScalar SUBTRACT_TINYINT = new MArithmetic("minus", "-", false, MNumeric.TINYINT, MNumeric.INT, 5) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int a0 = inputs.get(0).getInt8();
            int a1 = inputs.get(1).getInt8();
            output.putInt32(a0 - a1);
        }
    };

    public static final TScalar SUBTRACT_SMALLINT = new MArithmetic("minus", "-", false, MNumeric.SMALLINT, MNumeric.MEDIUMINT, 7) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int a0 = inputs.get(0).getInt16();
            int a1 = inputs.get(1).getInt16();
            output.putInt32(a0 - a1);
        }
    };

    public static final TScalar SUBTRACT_MEDIUMINT = new MArithmetic("minus", "-", false, MNumeric.MEDIUMINT, MNumeric.INT, 9) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int a0 = inputs.get(0).getInt32();
            int a1 = inputs.get(1).getInt32();
            output.putInt32(a0 - a1);
        }
    };

    public static final TScalar SUBTRACT_INT = new MArithmetic("minus", "-", false, MNumeric.INT, MNumeric.BIGINT, 12) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            long a0 = inputs.get(0).getInt32();
            long a1 = inputs.get(1).getInt32();
            output.putInt64(a0 - a1);
        }
    };

    public static final TScalar SUBTRACT_BIGINT = new MArithmetic("minus", "-", false, MNumeric.BIGINT, MNumeric.BIGINT, 21) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            long a0 = inputs.get(0).getInt64();
            long a1 = inputs.get(1).getInt64();
            output.putInt64(a0 - a1);
        }
    };

    public static final TScalar SUBTRACT_DECIMAL = new DecimalArithmetic("minus", "-", false) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            output.putObject(MBigDecimal.getWrapper(context, DEC_INDEX)
                        .set(MBigDecimal.getWrapper(inputs.get(0), context.inputTInstanceAt(1)))
                        .subtract(MBigDecimal.getWrapper(inputs.get(1), context.inputTInstanceAt(1))));
        }

        @Override
        protected long precisionAndScale(int arg0Precision, int arg0Scale, int arg1Precision, int arg1Scale) {
            return plusOrMinusArithmetic(arg0Precision, arg0Scale, arg1Precision, arg1Scale);
        }
    };

    public static final TScalar SUBSTRACT_DOUBLE = new MArithmetic("minus", "-", true, MApproximateNumber.DOUBLE, MApproximateNumber.DOUBLE)
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            output.putDouble(inputs.get(0).getDouble() - inputs.get(1).getDouble());
        }
    };

    // (Regular) Divide functions
    public static final TScalar DIVIDE_TINYINT = new MArithmetic("divide", "/", false, MNumeric.TINYINT, MApproximateNumber.DOUBLE)
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            int divisor = inputs.get(1).getInt8();
            if (divisor == 0)
                output.putNull();
            else
                output.putDouble((double)inputs.get(0).getInt8() / divisor);
        }
    };

    public static final TScalar DIVIDE_SMALLINT = new MArithmetic("divide", "/", false, MNumeric.SMALLINT, MApproximateNumber.DOUBLE)
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            int divisor = inputs.get(1).getInt16();
            if (divisor == 0)
                output.putNull();
            else
                output.putDouble((double)inputs.get(0).getInt16() / divisor);
        }
    };

    public static final TScalar DIVIDE_INT = new MArithmetic("divide", "/", false, MNumeric.INT, MApproximateNumber.DOUBLE)
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            int divisor = inputs.get(1).getInt32();
            if (divisor == 0L)
                output.putNull();
            else
                output.putDouble((double)inputs.get(0).getInt32() / divisor);
        }
    };

    public static final TScalar DIVIDE_BIGINT = new MArithmetic("divide", "/", false, MNumeric.BIGINT, MApproximateNumber.DOUBLE)
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            long divisor = inputs.get(1).getInt64();
            if (divisor == 0L)
                output.putNull();
            else
                output.putDouble((double)inputs.get(0).getInt64() / divisor);
        }
    };

    public static final TScalar DIVIDE_DOUBLE = new MArithmetic("divide", "/", false, MApproximateNumber.DOUBLE, MApproximateNumber.DOUBLE)
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            double divisor = inputs.get(1).getDouble();
            if (Double.compare(divisor, 0) == 0)
                output.putNull();
            else
                output.putDouble(inputs.get(0).getDouble() / divisor);
        }
    };

    public static final TScalar DIVIDE_DECIMAL = new DecimalArithmetic("divide", "/", false) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            BigDecimalWrapper divisor = MBigDecimal.getWrapper(inputs.get(1), context.inputTInstanceAt(1));

            if (divisor.isZero()) {
                output.putNull();
            }
            else {
                BigDecimalWrapper numerator = MBigDecimal.getWrapper(inputs.get(0), context.inputTInstanceAt(0));
                BigDecimalWrapper result = MBigDecimal.getWrapper(context, DEC_INDEX);
                result.set(numerator);
                result.divide(divisor, context.outputTInstance().attribute(Attrs.SCALE));
                output.putObject(result);
            }
        }

       @Override
       protected long precisionAndScale(int p1, int s1, int p2, int s2)
       {
           // https://dev.mysql.com/doc/refman/5.5/en/arithmetic-functions.html :
           //
           // In division performed with /, the scale of the result when using two exact-value operands is the scale of
           // the first operand plus the value of the div_precision_increment system variable (which is 4 by default).
           //
           // This seems to apply to precision, too.

           int precision = p1 + DIV_PRECISION_INCREMENT;
           int scale = s1 + DIV_PRECISION_INCREMENT;

           return packPrecisionAndScale(precision, scale);
       }
   };

   // integer division
    public static final TScalar DIV_TINYINT = new MArithmetic("div", "div", false, MNumeric.TINYINT, MNumeric.INT, 4)
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            int divisor = inputs.get(1).getInt8();
            if (divisor == 0)
                output.putNull();
            else
                output.putInt32(inputs.get(0).getInt8() / divisor);
        }
    };

    public static final TScalar DIV_SMALLINT = new MArithmetic("div", "div", false, MNumeric.SMALLINT, MNumeric.INT, 6)
    {

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            int divisor = inputs.get(1).getInt16();
            if (divisor == 0)
                output.putNull();
            else
                output.putInt32(inputs.get(0).getInt16() / divisor);
        }
    };

    public static final TScalar DIV_MEDIUMINT = new MArithmetic("div", "div", false, MNumeric.MEDIUMINT, MNumeric.INT, 9)
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            int divisor = inputs.get(1).getInt32();
            if (divisor == 0)
                output.putNull();
            else
                output.putInt32(inputs.get(0).getInt32() / divisor);
        }
    };

    public static final TScalar DIV_INT =new MArithmetic("div", "div", false, MNumeric.INT, MNumeric.INT, 11)
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            int divisor = inputs.get(1).getInt32();
            if (divisor == 0)
                output.putNull();
            else
                output.putInt32(inputs.get(0).getInt32() / divisor);
        }
    };

    public static final TScalar DIV_BIGINT = new MArithmetic("div", "div", false, MNumeric.BIGINT, MNumeric.BIGINT, 20)
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            long divisor = inputs.get(1).getInt64();
            if (divisor == 0L)
                output.putNull();
            else
                output.putInt64(inputs.get(0).getInt64() / divisor);
        }
    };

    public static final TScalar DIV_DOUBLE = new MArithmetic("div", "div", false, MApproximateNumber.DOUBLE, MNumeric.BIGINT, 22)
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            double divisor = inputs.get(1).getDouble();
            if (Double.compare(divisor, 0) == 0)
                output.putNull();
            else
                output.putInt64((long)(inputs.get(0).getDouble() / divisor));
        }
    };
    //(String overloadName, String infix, boolean associative, TClass inputType, TInstance resultType)
    public static final TScalar DIV_DECIMAL = new MArithmetic("div", "div", false, MNumeric.DECIMAL, null) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs,
                                  PValueTarget output) {
            BigDecimalWrapper wrapper = MBigDecimal.getWrapper(context, DEC_INDEX);
            BigDecimalWrapper numerator = MBigDecimal.getWrapper(inputs.get(0), context.inputTInstanceAt(0));
            BigDecimalWrapper divisor = MBigDecimal.getWrapper(inputs.get(1), context.inputTInstanceAt(1));
            long rounded = wrapper.set(numerator).divide(divisor).round(0).asBigDecimal().longValue();
            output.putInt64(rounded);
        }

        @Override
        public TOverloadResult resultType() {
            return TOverloadResult.custom(new TInstanceGenerator(MNumeric.BIGINT), new TCustomOverloadResult() {
                @Override
                public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                    TInstance numeratorType = inputs.get(0).instance();
                    int precision = numeratorType.attribute(Attrs.PRECISION);
                    int scale = numeratorType.attribute(Attrs.SCALE);

                    // These seem to be MySQL's wonky rules. For instance:
                    //  DECIMAL(11, 0) -> BIGINT(12)
                    //  DECIMAL(11, 1) -> BIGINT(12)
                    //  DECIMAL(11, 2) -> BIGINT(11)
                    //  DECIMAL(11, 3) -> BIGINT(10) etc
                    ++precision;
                    scale = (scale == 0) ? 0 : scale - 1;
                    TClass tClass = (scale > 11) ? MNumeric.BIGINT : MNumeric.INT;
                    return tClass.instance(precision - scale, anyContaminatingNulls(inputs));
                }
            });
        }
    };

    // Multiply functions
    public static final TScalar MULTIPLY_TINYINT = new MArithmetic("times", "*", true, MNumeric.TINYINT, MNumeric.INT, 7) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int a0 = inputs.get(0).getInt8();
            int a1 = inputs.get(1).getInt8();
            output.putInt32(a0 * a1);
        }
    };

    public static final TScalar MULTIPLY_SMALLINT = new MArithmetic("times", "*", true, MNumeric.SMALLINT, MNumeric.INT, 11) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int a0 = inputs.get(0).getInt16();
            int a1 = inputs.get(1).getInt16();
            output.putInt32(a0 * a1);
        }
    };

    public static final TScalar MULTIPLY_MEDIUMINT = new MArithmetic("times", "*", true, MNumeric.MEDIUMINT, MNumeric.BIGINT, 15) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            long a0 = inputs.get(0).getInt32();
            long a1 = inputs.get(1).getInt32();
            output.putInt64(a0 * a1);
        }
    };

    public static final TScalar MULTIPLY_INT = new MArithmetic("times", "*", true, MNumeric.INT, MNumeric.BIGINT, 21) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            long a0 = inputs.get(0).getInt32();
            long a1 = inputs.get(1).getInt32();
            output.putInt64(a0 * a1);
        }
    };

    public static final TScalar MULTIPLY_BIGINT = new MArithmetic("times", "*", true, MNumeric.BIGINT, MNumeric.BIGINT, 39) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            long a0 = inputs.get(0).getInt64();
            long a1 = inputs.get(1).getInt64();
            output.putInt64(a0 * a1);
        }
    };

    public static final TScalar MULTIPLY_DOUBLE = new MArithmetic("times", "*", true, MApproximateNumber.DOUBLE, MApproximateNumber.DOUBLE) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs,
                                  PValueTarget output) {
            double result = inputs.get(0).getDouble() * inputs.get(1).getDouble();
            if (!Doubles.isFinite(result))
                output.putNull();
            else
                output.putDouble(result);
        }

        @Override
        public int[] getPriorities() {
            return new int[] { 1, 2 };
        }
    };

    public static final TScalar MULTIPLY_DECIMAL = new DecimalArithmetic("times", "*", true)
    {
        @Override
        protected long precisionAndScale(int arg0Precision, int arg0Scale, int arg1Precision, int arg1Scale)
        {
            //TODO:
            // for now, just sum up the precisions and scales
            return  packPrecisionAndScale(arg0Precision + arg1Precision,
                                          arg0Scale + arg1Scale);
        }

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            BigDecimalWrapper wrapper = MBigDecimal.getWrapper(context, DEC_INDEX);
            BigDecimalWrapper arg0 = MBigDecimal.getWrapper(inputs.get(0), context.inputTInstanceAt(0));
            BigDecimalWrapper arg1 = MBigDecimal.getWrapper(inputs.get(1), context.inputTInstanceAt(1));
            
            output.putObject(wrapper.set(arg0).multiply(arg1));
        }
    };

    // mod function
    public static final TScalar MOD_TINYTINT = new MArithmetic("mod", "mod", false, MNumeric.TINYINT, MNumeric.INT, 4)
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            int right = inputs.get(1).getInt8();
            if (right == 0)
                output.putNull();
            else
                output.putInt32(inputs.get(0).getInt8() % right);
        }
    };
   
    public static final TScalar MOD_SMALLINT = new MArithmetic("mod", "mod", false, MNumeric.SMALLINT, MNumeric.INT, 6)
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            int right = inputs.get(1).getInt16();
            if (right == 0)
                output.putNull();
            else
                output.putInt32(inputs.get(0).getInt16() % right);
        }
    };
   
    public static final TScalar MOD_MEDIUMINT = new MArithmetic("mod", "mod", false, MNumeric.MEDIUMINT, MNumeric.INT, 9)
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            int right = inputs.get(1).getInt32();
            if (right == 0)
                output.putNull();
            else
                output.putInt32(inputs.get(0).getInt32() % right);
        }
    };
   
    public static final TScalar MOD_INT = new MArithmetic("mod", "mod", false, MNumeric.INT, MNumeric.INT, 11)
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            int right = inputs.get(1).getInt32();
            if (right == 0)
                output.putNull();
            else
                output.putInt32(inputs.get(0).getInt32() % right);
        }
    };
   
    public static final TScalar MOD_BIGINT = new MArithmetic("mod", "mod", false, MNumeric.BIGINT, MNumeric.BIGINT, 20)
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            long right = inputs.get(1).getInt64();
            if (right == 0L)
                output.putNull();
            else
                output.putInt64(inputs.get(0).getInt64() % right);
        }
    };
   
    public static final TScalar MOD_DOUBLE = new MArithmetic("mod", "mod", false, MApproximateNumber.DOUBLE, MApproximateNumber.DOUBLE)
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            double right = inputs.get(1).getDouble();
            if (Double.compare(right, 0) == 0)
                output.putNull();
            else
                output.putDouble(inputs.get(0).getDouble() % right);
        }
    };
   
    public static final TScalar MOD_DECIMAL = new DecimalArithmetic("mod", "mod", false)
    {
        @Override
        protected long precisionAndScale(int arg0Precision, int arg0Scale, int arg1Precision, int arg1Scale)
        {
            return packPrecisionAndScale(Math.max(arg0Precision, arg1Precision),
                                         Math.max(arg0Scale, arg1Scale));
        }

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {            
             BigDecimalWrapper divisor = MBigDecimal.getWrapper(inputs.get(1), context.inputTInstanceAt(1));

             if (divisor.isZero())
                 output.putNull();
             else
                 output.putObject(MBigDecimal.getWrapper(context, DEC_INDEX)
                                     .set(MBigDecimal.getWrapper(inputs.get(0), context.inputTInstanceAt(1)))
                                     .mod(divisor));
        }
    };
   
   // TODO this should extend some base class that MArithmetic also extends, rather than extending MArithmetic
   // but ignoring its TInstance field
    private abstract static class DecimalArithmetic extends MArithmetic {
        @Override
        public TOverloadResult resultType() {
           return TOverloadResult.custom(new TCustomOverloadResult() {
               @Override
               public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                   TInstance arg0 = inputs.get(0).instance();
                   TInstance arg1 = inputs.get(1).instance();

                   int arg0Precision = arg0.attribute(Attrs.PRECISION);
                   int arg0Scale = arg0.attribute(Attrs.SCALE);

                   int arg1Precision = arg1.attribute(Attrs.PRECISION);
                   int arg1Scale = arg1.attribute(Attrs.SCALE);
                   long resultPrecisionAndScale = precisionAndScale(arg0Precision, arg0Scale, arg1Precision, arg1Scale);
                   int resultPrecision = (int)(resultPrecisionAndScale >> 32);
                   int resultScale = (int)resultPrecisionAndScale;
                   return MNumeric.DECIMAL.instance(resultPrecision, resultScale, anyContaminatingNulls(inputs));
               }
           });
        }

        protected abstract long precisionAndScale(int arg0Precision, int arg0Scale, int arg1Precision, int arg1Scale);

        static long packPrecisionAndScale(int precision, int scale) {
            long result = precision;
            result <<= 32;
            result |= scale;
            return result;
        }

       @Override
       protected TInstanceNormalizer inputSetInstanceNormalizer() {
           return TInstanceNormalizers.ALL_UNTOUCHED;
       }

       static long plusOrMinusArithmetic(int arg0Precision, int arg0Scale, int arg1Precision, int arg1Scale){
            int maxScale = Math.max(arg0Scale, arg1Precision);
            int maxPrecision = Math.max(arg0Precision, arg1Precision);
            return packPrecisionAndScale(maxPrecision + maxScale, maxScale);
        }

        protected DecimalArithmetic(String overloadName, String infix, boolean associative) {
            super(overloadName, infix, associative, MNumeric.DECIMAL, null);
        }
    }

    /**
     * Implementation for arithmetic which always results in a NULL VARCHAR(29). Odd as it may seem, such things do
     * seem to exist. For instance, {@code &lt;time&gt; + INTERVAL N MONTH}.
     */
    static class AlwaysNull extends MArithmetic {

        AlwaysNull(String overloadName, String infix, boolean associative, TClass operand0, TClass operand1) {
            super(overloadName, infix, associative, operand0, operand1, MString.VARCHAR, 29);
        }

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs,
                                  PValueTarget output) {
            output.putNull();
        }

        @Override
        protected Constantness constness(int inputIndex, LazyList<? extends TPreptimeValue> values) {
            return Constantness.CONST;
        }

        @Override
        protected boolean nullContaminates(int inputIndex) {
            // We return false here so that TScalarBase never tries to look at the inputs as part of evaluating
            // a const, to see if they're null. If it did, a non-const input would cause an exception during const
            // evaluation. Returning false here means we'll always get to doEvaluate, which will then putNull.
            return false;
        }

    }

    
    // multiplications
    public static final TScalar MULT_MONTH_DOUBLE 
            = new IntervalArith(AkInterval.MONTHS, 0, MApproximateNumber.DOUBLE, 1, IntervalOp.MULT);
    public static final TScalar MULT_DOUBLE_MONTH 
            = new IntervalArith(AkInterval.MONTHS, 1, MApproximateNumber.DOUBLE, 0, IntervalOp.MULT);
    public static final TScalar MULT_SECS_DOUBLE 
            = new IntervalArith(AkInterval.SECONDS, 0, MApproximateNumber.DOUBLE, 1, IntervalOp.MULT);
    public static final TScalar MULT_DOUBLE_SECS 
            = new IntervalArith(AkInterval.SECONDS, 1, MApproximateNumber.DOUBLE, 0, IntervalOp.MULT);
    
    // divisions
    public static final TScalar DIVIDE_MONTHS_DOUBLE 
            = new IntervalArith(AkInterval.MONTHS, 0, MApproximateNumber.DOUBLE, 1, IntervalOp.DIVIDE);
    public static final TScalar DIVIDE_SECS_DOUBLE 
            = new IntervalArith(AkInterval.SECONDS, 0, MApproximateNumber.DOUBLE, 1, IntervalOp.DIVIDE);
    
    // additions
    public static final TScalar ADD_MONTH
            = new IntervalArith(AkInterval.MONTHS, 0, AkInterval.MONTHS, 1, IntervalOp.ADD);
  
    public static final TScalar ADD_DAY
            = new IntervalArith(AkInterval.SECONDS, 0, AkInterval.SECONDS, 1, IntervalOp.ADD);
    
    // substractions
    public static final TScalar SUBSTRACT_MONTH
            = new IntervalArith(AkInterval.MONTHS, 0, AkInterval.MONTHS, 1, IntervalOp.MINUS);
  
    public static final TScalar SUBSTRACT_DAY
            = new IntervalArith(AkInterval.SECONDS, 0, AkInterval.SECONDS, 1, IntervalOp.MINUS);
    
    private static enum IntervalOp
    {
        MULT("times")
        {
            @Override
            long doMath(LazyList<? extends PValueSource> inputs, int pos0, int pos1)
            {
                return Math.round(inputs.get(pos0).getInt64()
                                    * inputs.get(pos1).getDouble());
            }
        },
        DIVIDE("divide")
        {
            @Override
            long doMath(LazyList<? extends PValueSource> inputs, int pos0, int pos1)
            {
                assert pos0 == 0 && pos1 == 1 : "ONLY <INTERVA> / <NUMERIC> is supported";

                return Math.round(inputs.get(0).getInt64()
                                    / inputs.get(1).getDouble());
            }
        },
        ADD("plus")
        {
            @Override
            long doMath(LazyList<? extends PValueSource> inputs, int pos0, int pos1)
            {
                return inputs.get(0).getInt64() + inputs.get(1).getInt64();
            }
        },
        MINUS("minus")
        {
            @Override
            long doMath(LazyList<? extends PValueSource> inputs, int pos0, int pos1)
            {
                return inputs.get(0).getInt64() - inputs.get(1).getInt64();
            }
        }
        ;
        
        private IntervalOp(String n)
        {
            name = n;
        }
        
        final String name;
        abstract long doMath(LazyList<? extends PValueSource> inputs, int pos0, int pos1);
    }

    static class IntervalArith extends TScalarBase
    {
        private final TClass left, right;
        protected final int pos0, pos1;
        private final IntervalOp op;
        
        IntervalArith(TClass left,int pos0, TClass right, int pos1, IntervalOp op)
        {
           if (!(pos0 == 0 && pos1 == 1
                   || pos0 == 1 && pos1 == 0))
               throw new IllegalArgumentException("pos0 and pos1 must be {0, 1}");
           
           this.left = left;
           this.right = right;
           this.pos0 = pos0;
           this.pos1 = pos1;
           this.op = op;
        }
        
        @Override
        protected void buildInputSets(TInputSetBuilder builder)
        {
            builder.covers(left, pos0).covers(right, pos1);
        }

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            output.putInt64(op.doMath(inputs, pos0, pos1));
        }
        
        @Override
        public String displayName()
        {
            return op.name;
        }

        @Override
        public TOverloadResult resultType()
        {
            return TOverloadResult.custom(new TCustomOverloadResult()
            {
                @Override
                public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context)
                {
                    return inputs.get(pos0).instance();
                }   
            });
        }
    }
    
    private static final int DIV_PRECISION_INCREMENT = 4;
}
