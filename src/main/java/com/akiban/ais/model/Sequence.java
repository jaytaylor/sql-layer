/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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
package com.akiban.ais.model;

import java.util.concurrent.atomic.AtomicReference;

import com.akiban.ais.model.validation.AISInvariants;
import com.akiban.server.AccumulatorAdapter;
import com.akiban.server.AccumulatorAdapter.AccumInfo;
import com.akiban.server.error.SequenceLimitExceededException;
import com.akiban.server.service.tree.TreeCache;
import com.akiban.server.service.tree.TreeLink;
import com.persistit.Tree;
import com.persistit.exception.PersistitException;

public class Sequence implements TreeLink {

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
        this.range = maxValue - minValue + 1;
        this.cacheSize = 20;
    }
    
    public final TableName getSequenceName() {
        return sequenceName;
    }
    
    @Override 
    public final String getTreeName() {
        return treeName;
    }
    public final void setTreeName(String treeName) {
        this.treeName = treeName;
    }
    public final Integer getAccumIndex() {
        return accumIndex;
    }
    public final void setAccumIndex(int accumIndex) {
        this.accumIndex = accumIndex;
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
    protected String treeName;
    private Integer accumIndex;
    
    private final long startsWith;
    private final long increment;
    private final long minValue;
    private final long maxValue;
    private final boolean cycle;
    private final long cacheSize;
    private final long range;
    private AtomicReference<TreeCache> treeCache = new AtomicReference<>();
    
   
    // TreeLink implementation
    @Override
    public String getSchemaName() {
        return sequenceName.getSchemaName();
    }

    @Override
    public void setTreeCache(TreeCache cache) {
        treeCache.set(cache);
    }

    @Override
    public TreeCache getTreeCache() {
        return treeCache.get();
    }

    public long nextValueRaw(long rawSequence) {
        long nextValue = notCycled(rawSequence);
        if (nextValue > maxValue || nextValue < minValue) {
            if(!cycle) {
                throw new SequenceLimitExceededException(this);
            }
            nextValue = cycled(nextValue);
        }
        return nextValue;
    }

    public long currentValueRaw(long rawSequence) {
        return cycled(notCycled(rawSequence));
    }

    private long notCycled(long rawSequence) {
        // -1 so first is startsWith, second is startsWith+inc, etc
        return startsWith + ((rawSequence - 1) * increment);
    }

    private long cycled(long notCycled) {
        long mod = (notCycled - minValue) % range;
        if(mod < 0) {
            mod += range;
        }
        return minValue + mod;
    }
}
