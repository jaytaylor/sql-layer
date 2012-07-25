package com.akiban.server.types3.common.funcs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.TableName;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;

public class SequenceNextValue extends TOverloadBase {

    public static TOverload[] INSTANCES = SequenceNextValue.create ();
    
    public static TOverload[] create () {
        return new TOverload[]{
            new SequenceNextValue(MNumeric.BIGINT)
            }; 
    }
    protected final TClass inputType;
    
    private static final Logger logger = LoggerFactory.getLogger(SequenceNextValue.class);

    public SequenceNextValue (TClass returnType) {
        this.inputType = returnType;
    }

    @Override
    public String overloadName() {
        return "NEXTVAL";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(inputType.instance());
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(MString.VARCHAR, 0, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context,
            LazyList<? extends PValueSource> inputs, PValueTarget output) {

        String schema = inputs.get(0).isNull() ? context.getCurrentUser() : inputs.get(0).getString();
        String sequence = inputs.get(1).getString();

        TableName sequenceName = new TableName (schema, sequence);
        
        logger.warn("Sequence loading : "+ schema + "." + sequence);
        output.putInt64(0);
    }
    
    //private final Sequence sequence; 
/*    
    public SequenceNextValue() {
        super("NEXTVAL", false);
    }

    @Override
    public void evaluate(TExecutionContext context, PValueTarget target) {
       //target.putInt64(sequence.nextValue(null));
        target.putInt64(0);
        logger.debug("SequenceNextValue.evaluate: ");
        
        
    }

    @Override
    protected TInstance tInstance() {
        return MNumeric.BIGINT.instance();
    }
*/    
}
