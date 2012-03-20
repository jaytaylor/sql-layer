/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.optimizer.plan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public final class ConditionsStack<C> {
    private Collection<C> main;
    private List<Collection<C>> recycleStack;
    int frameIndex = -1;
    private int conditionsCount;
    
    public ConditionsStack(Collection<C> conditions, int expectedLevels) {
        conditionsCount = conditions.size();
        main = new HashSet<C>(conditions);
        recycleStack = new ArrayList<Collection<C>>(expectedLevels);
    }
    
    public void enter() {
        ++frameIndex;
        assert frameIndex <= recycleStack.size() : frameIndex + " > " + recycleStack;
        if (frameIndex == recycleStack.size()) {
            recycleStack.add(new ArrayList<C>(conditionsCount));
        }
    }

    public void leave() {
        Collection<C> frame = getFrame();
        main.addAll(frame);
        frame.clear();
        --frameIndex;
    }

    public boolean removedAny() {
        return ! getFrame().isEmpty();
    }
    
    public void removeCondition(C condition) {
        if (main.remove(condition)) {
            getFrame().add(condition);
        }
    }

    public List<C> getAllRemoved() {
        List<C> results = new ArrayList<C>(conditionsCount);
        for (Collection<C> frame : recycleStack)
            results.addAll(frame);
        return results;
    }

    private Collection<C> getFrame() {
        if (frameIndex < 0)
            throw new IllegalStateException("no active frame: " + frameIndex);
        return recycleStack.get(frameIndex);
    }
}
