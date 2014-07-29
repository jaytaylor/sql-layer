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
package com.foundationdb.ais.model;

import com.foundationdb.ais.model.validation.AISInvariants;
import com.foundationdb.server.error.SequenceLimitExceededException;

public class Sequence extends HasStorage {

    public static Sequence create (AkibanInformationSchema ais,
            String schemaName, 
            String sequenceName, 
            long start, 
            long increment, 
            long minValue,
            long maxValue,
            boolean cycle) {
        Sequence sequence = new Sequence (ais, schemaName, sequenceName, start, increment, minValue, maxValue, cycle);
        ais.addSequence(sequence);
        return sequence; 
    }

    /** Create a copy of <code>seq</code>. Internal data (e.g. tree name) is not copied. */
    public static Sequence create (AkibanInformationSchema ais, Sequence seq) {
        return create(ais, seq.sequenceName.getSchemaName(), seq.sequenceName.getTableName(),
                      seq.startsWith, seq.increment, seq.minValue, seq.maxValue, seq.cycle);
    }

    protected Sequence (AkibanInformationSchema ais,
            String schemaName, 
            String sequenceName, 
            long start, 
            long increment, 
            long minValue,
            long maxValue,
            boolean cycle) {
        ais.checkMutability();
        AISInvariants.checkNullName(schemaName, "Sequence", "schema name");
        AISInvariants.checkNullName(sequenceName, "Sequence", "table name");

        this.ais = ais;
        this.sequenceName = new TableName (schemaName, sequenceName);
        this.startsWith = start;
        this.increment = increment;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.cycle = cycle;
    }

    public final TableName getSequenceName() {
        return sequenceName;
    }
    
    public final long getStartsWith() {
        return startsWith;
    }
    public final long getIncrement() {
        return increment;
    }
    public final long getMinValue() {
        return minValue;
    }
    public final long getMaxValue() {
        return maxValue;
    }
    public final boolean isCycle() {
        return cycle;
    }
    
    // State
    protected final AkibanInformationSchema ais;
    protected final TableName sequenceName;
    
    private final long startsWith;
    private final long increment;
    private final long minValue;
    private final long maxValue;
    private final boolean cycle;

    // HasStorage implementation

    @Override
    public AkibanInformationSchema getAIS() {
        return ais;
    }

    @Override
    public String getTypeString() {
        return "Sequence";
    }

    @Override
    public String getNameString() {
        return sequenceName.toString();
    }

    @Override
    public String getSchemaName() {
        return sequenceName.getSchemaName();
    }

    /**
     * Compute the real sequence value for the given raw sequence number.
     * <p>
     *     For example, the Sequence that starts at 5 and increments by 3 will have a
     *     real value of 5 for the raw number 1, real value of 8 for raw number 2, etc.
     * </p>
     */
    public long realValueForRawNumber(long rawNumber) {
        final long value;
        if(rawNumber == 0) {
            // nextval never called, just return 0
            value = 0;
        } else if(!cycle) {
            // Common case. Value always includes start.
            value = startsWith + ((rawNumber - 1) * increment);
            if((value < minValue) || (value > maxValue)) {
                throw new SequenceLimitExceededException(this);
            }
        } else {
            // Otherwise two cases: pre and post cycle
            boolean isIncreasing = (increment > 0);
            long absInc = Math.abs(increment);
            long numPreCycle;
            if(isIncreasing) {
                numPreCycle = ((maxValue - startsWith) / absInc) + 1;
            } else {
                numPreCycle = ((startsWith - minValue) / absInc) + 1;
            }
            // Zero when Long min/max are min/max
            if((rawNumber <= numPreCycle) || (numPreCycle == 0)) {
                value = startsWith + ((rawNumber - 1) * increment);
            } else {
                // Offset to 0 for start of cycle and find value in range [0, max)
                long perCycle = ((maxValue - minValue) / absInc) + 1;
                long n = (rawNumber - numPreCycle - 1) % perCycle;
                value = (isIncreasing ? minValue : maxValue) + (n * increment);
            }
        }
        return value;
    }
}
