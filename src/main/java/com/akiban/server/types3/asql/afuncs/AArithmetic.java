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
package com.akiban.server.types3.asql.afuncs;


import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.asql.atypes.ANumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.common.TArithmetic;
import java.math.BigDecimal;

public class AArithmetic {   

    private AArithmetic() {}
    
    // Add functions
    TArithmetic ADD_TINYINT = new TArithmetic("+", ANumeric.TINYINT, ANumeric.MEDIUMINT) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int a0 = inputs.get(0).getInt8();
            int a1 = inputs.get(0).getInt8();
            output.putInt32(a0 + a1);
        }
    };
    
     TArithmetic ADD_SMALLINT = new TArithmetic("+", ANumeric.SMALLINT, ANumeric.MEDIUMINT) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int a0 = inputs.get(0).getInt16();
            int a1 = inputs.get(0).getInt16();
            output.putInt32(a0 + a1);
        }
    };
     
     TArithmetic ADD_MEDIUMINT = new TArithmetic("+", ANumeric.MEDIUMINT, ANumeric.INT) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int a0 = inputs.get(0).getInt32();
            int a1 = inputs.get(1).getInt32();       
            output.putInt32(a0 + a1);
        }
    };
     
    TArithmetic ADD_INT = new TArithmetic("+", ANumeric.INT, ANumeric.BIGINT) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            long a0 = inputs.get(0).getInt32();
            long a1 = inputs.get(1).getInt32();       
            output.putInt64(a0 + a1);
        }
    };
    
    TArithmetic ADD_BIGINT = new TArithmetic("+", ANumeric.BIGINT, ANumeric.BIGINT) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            long a0 = inputs.get(0).getInt64();
            long a1 = inputs.get(1).getInt64();
            output.putInt64(a0 + a1);
        }
    };
     
    TArithmetic ADD_DECIMAL = new TArithmetic("+", ANumeric.DECIMAL, ANumeric.DECIMAL) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            // TODO: Make this faster
            BigDecimal dec0 = (BigDecimal) inputs.get(0).getObject();
            BigDecimal dec1 = (BigDecimal) inputs.get(1).getObject();
            BigDecimal result = dec0.add(dec1);
            output.putObject(result);
        }
    };
    
    // Subtract functions
    TArithmetic SUBTRACT_TINYINT = new TArithmetic("-", ANumeric.TINYINT, ANumeric.MEDIUMINT) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int a0 = inputs.get(0).getInt8();
            int a1 = inputs.get(0).getInt8();
            output.putInt32(a0 - a1);
        }
    };
    
     TArithmetic SUBTRACT_SMALLINT = new TArithmetic("-", ANumeric.SMALLINT, ANumeric.MEDIUMINT) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int a0 = inputs.get(0).getInt16();
            int a1 = inputs.get(0).getInt16();
            output.putInt32(a0 - a1);
        }
    };
     
     TArithmetic SUBTRACT_MEDIUMINT = new TArithmetic("-", ANumeric.MEDIUMINT, ANumeric.INT) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int a0 = inputs.get(0).getInt32();
            int a1 = inputs.get(1).getInt32();       
            output.putInt32(a0 - a1);
        }
    };
     
    TArithmetic SUBTRACT_INT = new TArithmetic("-", ANumeric.INT, ANumeric.BIGINT) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            long a0 = inputs.get(0).getInt32();
            long a1 = inputs.get(1).getInt32();       
            output.putInt64(a0 - a1);
        }
    };
    
    TArithmetic SUBTRACT_BIGINT = new TArithmetic("-", ANumeric.BIGINT, ANumeric.BIGINT) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            long a0 = inputs.get(0).getInt64();
            long a1 = inputs.get(1).getInt64();
            output.putInt64(a0 - a1);
        }
    };
     
    TArithmetic SUBTRACT_DECIMAL = new TArithmetic("-", ANumeric.DECIMAL, ANumeric.DECIMAL) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            // TODO: Make this faster
            BigDecimal dec0 = (BigDecimal) inputs.get(0).getObject();
            BigDecimal dec1 = (BigDecimal) inputs.get(1).getObject();
            BigDecimal result = dec0.subtract(dec1);
            output.putObject(result);
        }
    };
    
    // Divide functions
    TArithmetic DIVIDE_DECIMAL = new TArithmetic("/", ANumeric.DECIMAL, ANumeric.DECIMAL) {
        @Override 
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            // TODO: Make this faster
            BigDecimal dec0 = (BigDecimal) inputs.get(0).getObject();
            BigDecimal dec1 = (BigDecimal) inputs.get(1).getObject();
            BigDecimal result = dec0.divide(dec1);
            output.putObject(result);
        }
    };
    
    // Multiply functions
    TArithmetic MULTIPLY_TINYINT = new TArithmetic("*", ANumeric.TINYINT, ANumeric.INT) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int a0 = inputs.get(0).getInt8();
            int a1 = inputs.get(1).getInt8();
            output.putInt32(a0 * a1);
        }
    };
    
    TArithmetic MULTIPLY_SMALLINT = new TArithmetic("*", ANumeric.SMALLINT, ANumeric.INT) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int a0 = inputs.get(0).getInt16();
            int a1 = inputs.get(1).getInt16();
            output.putInt32(a0 * a1);
        }
    };
    
    TArithmetic MULTIPLY_MEDIUMINT = new TArithmetic("*", ANumeric.MEDIUMINT, ANumeric.BIGINT) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            long a0 = inputs.get(0).getInt32();
            long a1 = inputs.get(1).getInt32();
            output.putInt64(a0 * a1);
        }
    };
    
    TArithmetic MULTIPLY_INT = new TArithmetic("*", ANumeric.INT, ANumeric.BIGINT) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            long a0 = inputs.get(0).getInt32();
            long a1 = inputs.get(1).getInt32();
            output.putInt64(a0 * a1);
        }
    };
    
    TArithmetic MULTIPLY_BIGINT = new TArithmetic("*", ANumeric.BIGINT, ANumeric.BIGINT) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            long a0 = inputs.get(0).getInt64();
            long a1 = inputs.get(1).getInt64();
            output.putInt64(a0 * a1);
        }
    };
    
    TArithmetic MULTIPLY_DECIMAL = new TArithmetic("*", ANumeric.DECIMAL, ANumeric.DECIMAL) {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            long a0 = inputs.get(0).getInt64();
            long a1 = inputs.get(1).getInt64();
            output.putInt64(a0 * a1);
        }
    };
}