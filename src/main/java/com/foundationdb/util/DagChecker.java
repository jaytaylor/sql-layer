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

package com.foundationdb.util;

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
        DirectedGraph<T, Pair> graph = new DefaultDirectedGraph<>(Pair.class);

        Set<? extends T> initialNodes = initialNodes();
        Set<T> knownNodes = new HashSet<>(initialNodes.size() * 10); // just a guess
        Deque<T> nodePath = new ArrayDeque<>(20); // should be plenty
        boolean isDag = tryAdd(initialNodes, graph, knownNodes, new CycleDetector<>(graph), nodePath);
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
        return badNodes == null ? null : new ArrayList<>(badNodes);
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
