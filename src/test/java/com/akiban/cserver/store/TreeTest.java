/**
 * 
 */
package com.akiban.cserver.store;

import static org.junit.Assert.*;

import org.junit.Test;

import com.akiban.cserver.store.IVisitor;
import com.akiban.cserver.store.Tree;

import java.util.*;

/**
 * @author percent
 *
 */
public class TreeTest {

    public class Order {
        public Order(int e, int o) {
            element = e ;
            order = o;
        }
        public int element;
        public int order;
    }
    
    public class TestVisitor implements IVisitor {
        
        public TestVisitor() {
            history = new ArrayList<Order>();
            count = 0;
            upCount = 0;
            downCount = 0;
        }

        public void up() {
            upCount ++;
        }
        
        public void down() {
            downCount ++;
        }

        @Override
        public boolean visit(Object n) {
            assertTrue(n instanceof Tree<?>);
            Integer node = ((Tree<Integer>)n).getNode();
            history.add(new Order(node, count++));
            return false;
        }        
        public ArrayList<Order> history;
        public int count;
        public int upCount;
        public int downCount;

    }
    
    /**
     * Test method for {@link com.akiban.cserver.store.Tree#Tree(java.lang.Object)}.
     */
    @Test
    public void testTree() {
        TestVisitor tv = new TestVisitor();
        Tree<Integer> t = new Tree<Integer>(new Integer(0));
        Tree<Integer> t1 = new Tree<Integer>(new Integer(1));
        Tree<Integer> t2 = new Tree<Integer>(new Integer(2));
        Tree<Integer> t3 = new Tree<Integer>(new Integer(3));
        Tree<Integer> t4 = new Tree<Integer>(new Integer(4));
        Tree<Integer> t5 = new Tree<Integer>(new Integer(5));
        Tree<Integer> t6 = new Tree<Integer>(new Integer(6));
        Tree<Integer> t7 = new Tree<Integer>(new Integer(7));
        Tree<Integer> t8 = new Tree<Integer>(new Integer(8));
        
        t.add(t1);
        t1.add(t2);
        t2.add(t3);
        t1.add(t4);
        t.add(t5);
        t.add(t6);
        t6.add(t7);
        t6.add(t8);
        t.accept(tv);
        
        Iterator<Order> i = tv.history.iterator();
        int count = 0;
        while(i.hasNext()) {
            Order o = i.next();
            assertEquals(count, o.element);
            assertEquals(count++, o.order);
        }
    }

}
