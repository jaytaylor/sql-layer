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
import com.akiban.server.service.tree.TreeService;
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

    // State
    protected final TableName sequenceName;
    protected String treeName;
    private Integer accumIndex;
    
    private final long startsWith;
    private final long increment;
    private final long minValue;
    private final long maxValue;
    private final boolean cycle;

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
    
    public long nextValue(TreeService treeService) throws PersistitException {
        
        Tree tree = getTreeCache().getTree();
        AccumulatorAdapter accum = new AccumulatorAdapter (AccumInfo.AUTO_INC, treeService, tree);
        long value = accum.updateAndGet(increment);
        
        if (value > maxValue && increment > 0) {
            if (cycle) {
                value = minValue;
                accum.set(value);
            } else {
                throw new SequenceLimitExceededException(this);
            }
        } else if (value < minValue && increment < 0) {
            if (cycle) {
                value = maxValue;
                accum.set(value);
            } else {
                throw new SequenceLimitExceededException (this);
            }
        }
        return value;
    }

    public long currentValue(TreeService treeService) throws PersistitException {
        Tree tree = getTreeCache().getTree();
        AccumulatorAdapter accum = new AccumulatorAdapter (AccumInfo.AUTO_INC, treeService, tree);
        return accum.getSnapshot(AccumInfo.AUTO_INC, treeService, tree);
    }
    
    public void setStartWithAccumulator(TreeService treeService) throws PersistitException {
        Tree tree = getTreeCache().getTree();
        AccumulatorAdapter accum = new AccumulatorAdapter (AccumInfo.AUTO_INC, treeService, tree);
        // Set the starting value to startsWith - increment, 
        // which will be, on first call to nextValue() be updated to the start value
        // TODO: This can cause problems if startsWith is within increment of 
        // Long.MaxValue or Long.MinValue. 
        accum.set(startsWith - increment);
    }
}
