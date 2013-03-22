package com.akiban.server.types3.mcompat.mcasts;

import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TCastBase;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

public final class Cast_From_Float {
    public static final TCast TO_DOUBLE_UNSIGNED = new TCastBase(MApproximateNumber.FLOAT, MApproximateNumber.FLOAT_UNSIGNED) {
        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            float orig = source.getFloat();
            if (orig < 0) {
                context.reportTruncate(Float.toString(orig), "0");
                orig = 0; // TODO or is it null?
            }
            target.putFloat(orig);
        }
    };

    private Cast_From_Float() {}
}
