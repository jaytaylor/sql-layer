
package com.akiban.server.types3.common.funcs;

import com.akiban.server.types3.texpressions.TScalarBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.model.TableName;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;

public class SequenceCurrentValue extends TScalarBase {

    public static final TScalar INSTANCE = new SequenceCurrentValue(MNumeric.BIGINT);

    protected final TClass inputType;
    
    private static final Logger logger = LoggerFactory.getLogger(SequenceCurrentValue.class);

    private SequenceCurrentValue (TClass returnType) {
        this.inputType = returnType;
    }

    @Override
    public String displayName() {
        return "CURRVAL";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(inputType);
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(MString.VARCHAR, 0, 1);
    }

    @Override
    protected boolean neverConstant() {
        return true;
    }

    @Override
    protected void doEvaluate(TExecutionContext context,
            LazyList<? extends PValueSource> inputs, PValueTarget output) {
        String schema = inputs.get(0).getString();
        String sequence = inputs.get(1).getString();
        logger.debug("Sequence loading : {}.{}", schema, sequence);

        TableName sequenceName = new TableName (schema, sequence);
        
        long value = context.sequenceCurrentValue(sequenceName);
        
        output.putInt64(value);
    }
}
