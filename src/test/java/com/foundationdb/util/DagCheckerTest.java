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

import org.jgrapht.EdgeFactory;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class DagCheckerTest {

    @Test
    public void empty() {
        new GraphFactory().isDag();
    }

    @Test
    public void line() {
        new GraphFactory().connect("a", "b").connect("b", "c").isDag();
    }

    @Test
    public void circle() {
        new GraphFactory().connect("a", "b").connect("b", "a").notDag("a", "b", "a");
    }

    @Test
    public void circlePlusShortCircuit() {
        new GraphFactory()
                .connect("a", "b")
                .connect("b", "c")
                .connect("c", "d")
                .connect("b", "d")
                .connect("d", "a")
                .notDag("a", "b", "c", "d", "a");
    }

    @Test
    public void split() {
        new GraphFactory()
                .connect("a", "b1").connect("b1", "c")
                .connect("a", "b2").connect("b2", "c")
                .isDag();
    }

    private static class GraphFactory {

        public GraphFactory connect(String from, String to) {
            graph.addVertex(from);
            graph.addVertex(to);
            graph.addEdge(from, to);
            return this;
        }

        public void isDag() {
            DagChecker<String> checker = getChecker();
            assertTrue("expected DAG for " + graph, checker.isDag());
            assertEquals("bad nodes", null, checker.getBadNodePath());
        }

        public void notDag(String... vertices) {
            DagChecker<String> checker = getChecker();
            assertFalse("expected non-DAG for " + graph, checker.isDag());
            List<String> expectedList = Arrays.asList(vertices);
            assertEquals("cycle path", expectedList, checker.getBadNodePath());
        }

        private DagChecker<String> getChecker() {
            return new DagChecker<String>() {
                @Override
                protected Set<? extends String> initialNodes() {
                    return new TreeSet<>(graph.vertexSet());
                }

                @Override
                protected Set<? extends String> nodesFrom(String starting) {
                    Set<String> candidates = new TreeSet<>(graph.vertexSet());
                    for (Iterator<String> iter = candidates.iterator(); iter.hasNext(); ) {
                        String vertex = iter.next();
                        if (vertex.equals(starting) || (!graph.containsEdge(starting, vertex)))
                            iter.remove();
                    }
                    return candidates;
                }
            };
        }

        private Graph<String, StringPair> graph = new DefaultDirectedGraph<>(factory);
    }

    private static final EdgeFactory<String, StringPair> factory = new EdgeFactory<String, StringPair>() {
        @Override
        public StringPair createEdge(String s, String s1) {
            return new StringPair(s, s1);
        }
    };

    private static class StringPair {

        @Override
        public String toString() {
            return from + " -> " + to;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            StringPair that = (StringPair) o;
            return from.equals(that.from) && to.equals(that.to);

        }

        @Override
        public int hashCode() {
            int result = from.hashCode();
            result = 31 * result + to.hashCode();
            return result;
        }

        private StringPair(String from, String to) {
            this.from = from;
            this.to = to;
        }

        private String from;
        private String to;
    }
}
