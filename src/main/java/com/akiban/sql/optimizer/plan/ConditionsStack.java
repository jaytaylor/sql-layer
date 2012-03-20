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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;

public final class ConditionsStack<C> {
    private Collection<C> main;
    private Deque<Collection<C>> recycleStack;
    private Queue<Collection<C>> recycleFlywheel;
    private int conditionsCount;
    
    public ConditionsStack(Collection<C> conditions, int expectedLevels) {
        conditionsCount = conditions.size();
        main = new HashSet<C>(conditions);
        recycleStack = new ArrayDeque<Collection<C>>(expectedLevels);
        recycleFlywheel = new ArrayDeque<Collection<C>>(expectedLevels);
    }
    
    public void enter() {
        Collection<C> frame = recycleFlywheel.poll();
        if (frame == null)
            frame = new ArrayList<C>(conditionsCount);
        recycleStack.push(frame);
    }

    public void leave() {
        Collection<C> frame = recycleStack.pop();
        main.addAll(frame);
        frame.clear();
        recycleFlywheel.offer(frame);
    }
    
    public boolean removedAny() {
        return ! recycleStack.peek().isEmpty();
    }
    
    public void removeCondition(C condition) {
        if (main.remove(condition)) {
            Collection<C> frame = recycleStack.peek();
            frame.add(condition);
        }
    }

    public List<C> getAllRemoved() {
        List<C> results = new ArrayList<C>(conditionsCount);
        for (Collection<C> frame : recycleStack)
            results.addAll(frame);
        return results;
    }
}
