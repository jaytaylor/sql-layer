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

package com.akiban.util;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class DagChecker<T> {
    protected abstract Set<? extends T> initialNodes();
    protected abstract Set<? extends T> nodesFrom(T starting);

    public boolean isDag() {
        Set<? extends T> initial = initialNodes();

        for (T starting : initial) {
            if (!checkFrom(starting, initial.size()))
                return false;
        }
        badNodes = null;
        return true;
    }

    public List<T> getBadNodePath() {
        return badNodes == null ? null : new ArrayList<T>(badNodes);
    }

    private boolean checkFrom(T starting, int size) {
        badNodes = new ArrayDeque<T>(size);
        return checkFrom(starting, new HashSet<T>(size));
    }

    private boolean checkFrom(T starting, Set<T> seen) {
        badNodes.addLast(starting);
        if (!seen.add(starting)) {
            return false;
        }
        for (T endpoint : nodesFrom(starting)) {
            if (!checkFrom(endpoint, seen))
                return false;
        }
        T removed = badNodes.removeLast();
        assert removed == starting : "expected " + starting + " but saw " + removed;
        return true;
    }

    private Deque<T> badNodes;
}
