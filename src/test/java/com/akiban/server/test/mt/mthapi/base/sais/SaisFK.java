/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.test.mt.mthapi.base.sais;

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
