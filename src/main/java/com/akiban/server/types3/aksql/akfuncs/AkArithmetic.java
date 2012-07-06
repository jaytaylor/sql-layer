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
package com.akiban.server.types3.aksql.akfuncs;


import com.akiban.server.error.DivisionByZeroException;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.aksql.aktypes.AkNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.common.funcs.TArithmetic;

public class AkArithmetic {

    private AkArithmetic() {}
    
    // Add functions
     public static final TOverload ADD_SMALLINT = new TArithmetic("+", AkNumeric.SMALLINT, AkNumeric.SMALLINT) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            short a0 = inputs.get(0).getInt16();
            short a1 = inputs.get(0).getInt16();
            output.putInt32(a0 + a1);
        }
    };
     
    public static final TOverload ADD_INT = new TArithmetic("+", AkNumeric.INT, AkNumeric.INT) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int a0 = inputs.get(0).getInt32();
            int a1 = inputs.get(1).getInt32();       
            output.putInt32(a0 + a1);
        }
    };
    
    public static final TOverload ADD_BIGINT = new TArithmetic("+", AkNumeric.BIGINT, AkNumeric.BIGINT) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            long a0 = inputs.get(0).getInt64();
            long a1 = inputs.get(1).getInt64();
            output.putInt64(a0 + a1);
        }
    };

    public static final TOverload ADD_DOUBLE = new TArithmetic("+", AkNumeric.DOUBLE, AkNumeric.DOUBLE) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs,
                                  PValueTarget output) {
            double a0 = inputs.get(0).getDouble();
            double a1 = inputs.get(1).getDouble();
            output.putDouble(a0 + a1);
        }
    };
    
    // Subtract functions
    public static final TOverload SUBTRACT_SMALLINT = new TArithmetic("-", AkNumeric.SMALLINT, AkNumeric.SMALLINT) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            short a0 = inputs.get(0).getInt16();
            short a1 = inputs.get(0).getInt16();
            output.putInt32(a0 - a1);
        }
    };
     
   public static final TOverload SUBTRACT_INT = new TArithmetic("-", AkNumeric.INT, AkNumeric.INT) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int a0 = inputs.get(0).getInt32();
            int a1 = inputs.get(1).getInt32();       
            output.putInt32(a0 - a1);
        }
    };
    
   public static final TOverload SUBTRACT_BIGINT = new TArithmetic("-", AkNumeric.BIGINT, AkNumeric.BIGINT) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            long a0 = inputs.get(0).getInt64();
            long a1 = inputs.get(1).getInt64();
            output.putInt64(a0 - a1);
        }
    };

   public static final TOverload SUBTRACT_DOUBLE = new TArithmetic("-", AkNumeric.DOUBLE, AkNumeric.DOUBLE) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs,
                                  PValueTarget output) {
            double a0 = inputs.get(0).getDouble();
            double a1 = inputs.get(1).getDouble();
            output.putDouble(a0 - a1);
        }
    };
    
    // Divide functions
   public static final TOverload DIVIDE_SMALLINT = new TArithmetic("/", AkNumeric.SMALLINT, AkNumeric.SMALLINT) {
        @Override 
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            short a1 = inputs.get(1).getInt16();
            
            if (a1 == 0) 
            {
                context.warnClient(new DivisionByZeroException());
                output.putNull();
            }
            else
                output.putInt32(inputs.get(0).getInt16() /a1);
            
        }
    };
    
   public static final TOverload DIVIDE_INT = new TArithmetic("/", AkNumeric.INT, AkNumeric.INT) {
        @Override 
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int a1 = inputs.get(1).getInt32();
            
            if (a1 == 0)
            {
                context.warnClient(new DivisionByZeroException());
                output.putNull();
            }
            else
                output.putInt32(inputs.get(0).getInt32() / a1);
            
        }
    };
    
   public static final TOverload DIVIDE_BIGINT = new TArithmetic("/", AkNumeric.BIGINT, AkNumeric.BIGINT) {
        @Override 
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            long a1 = inputs.get(1).getInt64();
            
            if (a1 == 0) 
            {
                context.warnClient(new DivisionByZeroException());
                output.putNull();
            }
            else
                output.putInt64(inputs.get(0).getInt64() / a1);
        }
    };

   public static final TOverload DIVIDE_DOUBLE = new TArithmetic("/", AkNumeric.DOUBLE, AkNumeric.DOUBLE) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs,
                                  PValueTarget output) {
            double a1 = inputs.get(1).getDouble();
            
            if (Double.compare(a1, 0) == 0) 
            {
                context.warnClient(new DivisionByZeroException());
                output.putNull();
            }
            else
                output.putDouble(inputs.get(0).getDouble() / a1);
        }
    };
    
    // Multiply functions
   public static final TOverload MULTIPLY_SMALLINT = new TArithmetic("*", AkNumeric.SMALLINT, AkNumeric.SMALLINT) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            short a0 = inputs.get(0).getInt16();
            short a1 = inputs.get(1).getInt16();
            output.putInt32(a0 * a1);
        }
    };

   public static final TOverload MULTIPLY_INT = new TArithmetic("*", AkNumeric.INT, AkNumeric.INT) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int a0 = inputs.get(0).getInt32();
            int a1 = inputs.get(1).getInt32();
            output.putInt32(a0 * a1);
        }
    };
    
   public static final TOverload MULTIPLY_BIGINT = new TArithmetic("*", AkNumeric.BIGINT, AkNumeric.BIGINT) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            long a0 = inputs.get(0).getInt64();
            long a1 = inputs.get(1).getInt64();
            output.putInt64(a0 * a1);
        }
    };

    TOverload MULTIPLY_DOUBLE = new TArithmetic("*", AkNumeric.DOUBLE, AkNumeric.DOUBLE) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs,
                                  PValueTarget output) {
            double a0 = inputs.get(0).getDouble();
            double a1 = inputs.get(1).getDouble();
            output.putDouble(a0 * a1);
        }
    };
    
    // Mod functions
   public static final TOverload MOD_DOUBLE = new TArithmetic("%", AkNumeric.DOUBLE, AkNumeric.DOUBLE.instance())
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            output.putDouble(inputs.get(0).getDouble() % inputs.get(1).getDouble());
        }
    };

   public static final TOverload MOD_SMALLINT = new TArithmetic("%", AkNumeric.SMALLINT, AkNumeric.SMALLINT.instance())
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            output.putDouble(inputs.get(0).getInt16() % inputs.get(1).getInt16());
        }
    };
    
   public static final TOverload MOD_INT = new TArithmetic("%", AkNumeric.INT, AkNumeric.INT.instance())
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            output.putInt32(inputs.get(0).getInt32() % inputs.get(0).getInt32());
        }
    };
    
   public static final TOverload MOD_BIGINT = new TArithmetic("%", AkNumeric.BIGINT, AkNumeric.BIGINT.instance())
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            output.putInt64(inputs.get(0).getInt64() % inputs.get(1).getInt64());
        }
    };
}