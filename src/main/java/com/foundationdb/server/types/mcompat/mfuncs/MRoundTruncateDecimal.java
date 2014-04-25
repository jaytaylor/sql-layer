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
package com.foundationdb.server.types.mcompat.mfuncs;

import com.foundationdb.server.types.*;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.common.types.DecimalAttribute;
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class MRoundTruncateDecimal extends TScalarBase {

    public static final Collection<TScalar> overloads = createAll();

    private static final int DEC_INDEX = 0;

    private enum RoundingStrategy {
        ROUND {
            @Override
            protected void apply(BigDecimalWrapper io, int scale) {
                io.round(scale);
            }
        },
        TRUNCATE {
            @Override
            protected void apply(BigDecimalWrapper io, int scale) {
                io.truncate(scale);
            }
        };

        protected abstract void apply(BigDecimalWrapper io, int scale);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        BigDecimalWrapper result = TBigDecimal.getWrapper(context, DEC_INDEX);
        result.set(TBigDecimal.getWrapper(inputs.get(0), context.inputTypeAt(0)));
        int scale = signatureStrategy.roundToScale(inputs);
        roundingStrategy.apply(result, scale);
        output.putObject(result);
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.custom(new TCustomOverloadResult() {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                TPreptimeValue valueToRound = inputs.get(0);
                ValueSource roundToPVal = signatureStrategy.getScaleOperand(inputs);
                int precision, scale;
                if ((roundToPVal == null) || roundToPVal.isNull()) {
                    precision = 17;
                    int incomingScale = valueToRound.type().attribute(DecimalAttribute.SCALE);
                    if (incomingScale > 1)
                        precision += (incomingScale - 1);
                    scale = incomingScale;
                } else {
                    scale = roundToPVal.getInt32();

                    TInstance incomingInstance = valueToRound.type();
                    int incomingPrecision = incomingInstance.attribute(DecimalAttribute.PRECISION);
                    int incomingScale = incomingInstance.attribute(DecimalAttribute.SCALE);

                    precision = incomingPrecision;
                    if (incomingScale > 1)
                        precision -= (incomingScale - 1);
                    precision += scale;

                }
                return MNumeric.DECIMAL.instance(precision, scale, anyContaminatingNulls(inputs));
            }
        });
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        signatureStrategy.buildInputSets(MNumeric.DECIMAL, builder);
    }

    @Override
    public String displayName() {
        return roundingStrategy.name();
    }

    protected MRoundTruncateDecimal(RoundingOverloadSignature signatureStrategy,
                                    RoundingStrategy roundingStrategy)
    {
        this.signatureStrategy = signatureStrategy;
        this.roundingStrategy = roundingStrategy;
    }

    private static Collection<TScalar> createAll() {
        List<TScalar> results = new ArrayList<>();
        for (RoundingOverloadSignature signature : RoundingOverloadSignature.values()) {
            for (RoundingStrategy rounding : RoundingStrategy.values()) {
                results.add(new MRoundTruncateDecimal(signature, rounding));
            }
        }
        return Collections.unmodifiableCollection(results);
    }

    private final RoundingOverloadSignature signatureStrategy;
    private final RoundingStrategy roundingStrategy;
}
