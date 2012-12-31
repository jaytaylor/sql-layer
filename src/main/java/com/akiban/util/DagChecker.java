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

import com.google.common.base.Objects;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.alg.CycleDetector;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class DagChecker<T> {
    protected abstract Set<? extends T> initialNodes();
    protected abstract Set<? extends T> nodesFrom(T starting);

    public boolean isDag() {
        DirectedGraph<T, Pair> graph = new DefaultDirectedGraph<T, Pair>(Pair.class);

        Set<? extends T> initialNodes = initialNodes();
        Set<T> knownNodes = new HashSet<T>(initialNodes.size() * 10); // just a guess
        Deque<T> nodePath = new ArrayDeque<T>(20); // should be plenty
        boolean isDag = tryAdd(initialNodes, graph, knownNodes, new CycleDetector<T, Pair>(graph), nodePath);
        if (!isDag) {
            this.badNodes = nodePath;
        }
        return isDag;
    }

    private boolean tryAdd(Set<? extends T> roots, Graph<T, Pair> graph, Set<T> knownNodes,
                           CycleDetector<T, Pair> cycleDetector, Deque<T> nodePath)
    {
        for (T node : roots) {
            nodePath.addLast(node);
            graph.addVertex(node);
            if (knownNodes.add(node)) {
                Set<? extends T> nodesFrom = nodesFrom(node);
                for (T from : nodesFrom) {
                    graph.addVertex(from);
                    Pair edge = new Pair(from, node);
                    graph.addEdge(from, node, edge);
                    nodePath.addLast(from);
                    if (cycleDetector.detectCycles())
                        return false;
                    nodePath.removeLast();
                }
                if (!tryAdd(nodesFrom, graph, knownNodes, cycleDetector, nodePath))
                    return false;
            }
            nodePath.removeLast();
        }
        return true;
    }

    public List<T> getBadNodePath() {
        return badNodes == null ? null : new ArrayList<T>(badNodes);
    }

    private Deque<T> badNodes = null;

    private static class Pair {

        private Pair(Object from, Object to) {
            this.from = from;
            this.to = to;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Pair pair = (Pair) o;
            return Objects.equal(this.from,  pair.from) && Objects.equal(this.to, pair.to);
        }

        @Override
        public int hashCode() {
            int result = from != null ? from.hashCode() : 0;
            result = 31 * result + (to != null ? to.hashCode() : 0);
            return result;
        }

        private Object from;
        private Object to;
    }
}
