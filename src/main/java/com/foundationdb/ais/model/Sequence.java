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

import java.util.concurrent.atomic.AtomicReference;

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
        AISInvariants.checkDuplicateSequence(ais, schemaName, sequenceName);

        this.sequenceName = new TableName (schemaName, sequenceName);
        this.startsWith = start;
        this.increment = increment;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.cycle = cycle;
        this.cacheSize = 20;
    }
    
    @Override
    public String toString()
    {
        return "Sequence(" + sequenceName + ")";
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
    public final long getCacheSize() {
        return cacheSize;
    }
    
    // State
    protected final TableName sequenceName;
    
    private final long startsWith;
    private final long increment;
    private final long minValue;
    private final long maxValue;
    private final boolean cycle;
    private final long cacheSize;

    // HasStorage implementation
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
        // Note: For Java MIN and MAX extents, addition in rawToReal takes care of cycling.
        long value = rawToReal(rawNumber);
        if(value > maxValue || value < minValue) {
            if(!cycle) {
                throw new SequenceLimitExceededException(this);
            }
            value = cycled(value);
        }
        return value;
    }

    private long rawToReal(long rawNumber) {
        // -1 so first is startsWith, second is startsWith+inc, etc
        return startsWith + ((rawNumber - 1) * increment);
    }

    private long cycled(long notCycled) {
        long range = maxValue - minValue + 1;
        long mod = (notCycled - minValue) % range;
        if(mod < 0) {
            mod += range;
        }
        return minValue + mod;
    }
}
