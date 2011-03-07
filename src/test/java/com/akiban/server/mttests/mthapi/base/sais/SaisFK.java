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

package com.akiban.server.mttests.mthapi.base.sais;

import com.akiban.util.ArgumentValidation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class SaisFK {
    private final SaisTable child;
    private final List<FKPair> fkFields;
    private AtomicReference<SaisTable> parent;

    SaisFK(SaisTable child, List<FKPair> fkFields) {
        ArgumentValidation.isGTE("fkFields", fkFields.size(), 1);
        this.child = child;
        this.fkFields = Collections.unmodifiableList(new ArrayList<FKPair>(fkFields));
        this.parent = new AtomicReference<SaisTable>(null);
    }

    public SaisTable getChild() {
        return child;
    }

    public List<FKPair> getFkPairs() {
        return fkFields;
    }

    @Override
    public String toString() {
        return String.format("FK[to %s]", getFkPairs());
    }

    public Iterator<String> getChildCols() {
        return new FKSIterator(Mode.CHILD);
    }

    public Iterator<String> getParentCols() {
        return new FKSIterator(Mode.PARENT);
    }

    public List<String> getChildColsList() {
        return iter2List(getChildCols());
    }

    public List<String> getParentColsList() {
        return iter2List(getParentCols());
    }

    private static List<String> iter2List(Iterator<String> iter) {
        List<String> cols = new ArrayList<String>();
        while(iter.hasNext()) {
            cols.add(iter.next());
        }
        return cols;
    }

    void setParent(SaisTable parent) {
        ArgumentValidation.notNull("parent", parent);
        if (!parent.getChildren().contains(this)) {
            throw new IllegalArgumentException(parent + " children don't include me! I'm: " + this);
        }
        if (!this.parent.compareAndSet(null, parent)) {
            throw new IllegalStateException("can't set ParentFK twice");
        }
    }

    public SaisTable getParent() {
        return parent.get();
    }

    static enum Mode {
        CHILD, PARENT
    }

    private class FKSIterator implements Iterator<String> {

        private final Mode mode;
        private final Iterator<FKPair> pairsIterator;

        protected FKSIterator(Mode mode) {
            this.pairsIterator = getFkPairs().iterator();
            this.mode = mode;
        }

        @Override
        public boolean hasNext() {
            return pairsIterator.hasNext();
        }

        @Override
        public String next() {
            FKPair fkPair = pairsIterator.next();
            switch (mode) {
                case CHILD:
                    return fkPair.getChild();
                case PARENT:
                    return fkPair.getParent();
            }
            throw new AssertionError(mode);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
