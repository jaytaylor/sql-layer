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

import com.google.common.collect.Sets;
import org.jgrapht.EdgeFactory;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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
        new GraphFactory().connect("a", "b").connect("b", "a").notDag("a", "b");
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

        }

        private DagChecker<String> getChecker() {
            return new DagChecker<String>() {
                @Override
                protected Set<? extends String> initialNodes() {
                    return graph.vertexSet();
                }

                @Override
                protected Set<? extends String> nodesFrom(String starting) {
                    Set<String> candidates = new HashSet<String>(graph.vertexSet());
                    for (Iterator<String> iter = candidates.iterator(); iter.hasNext(); ) {
                        String vertex = iter.next();
                        if (vertex.equals(starting) || (!graph.containsEdge(starting, vertex)))
                            iter.remove();
                    }
                    return candidates;
                }
            };
        }

        private Graph<String, StringPair> graph = new DefaultDirectedGraph<String, StringPair>(factory);
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
