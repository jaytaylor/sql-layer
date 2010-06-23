/**
 * 
 */
package com.akiban.cserver.store;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author percent
 *
 */
public class Tree<T> {
    public Tree(T root) {
        parent = root;
        children = new ArrayList<Tree<T>>();
    }

    public void add(Tree<T> child) {
        children.add(child);
    }
    
    public T getNode() {
        return parent;
    }
    
    public ArrayList<Tree<T>> getChildren() {
        return children;
    }
    
    public void accept(IVisitor v) {
        boolean done = v.visit(this);
        
        if(done) {
            return;
        }
        
        Iterator<Tree<T>> i = children.iterator();
        v.up();
        while(i.hasNext()) {
            Tree<T> child = i.next();
            child.accept(v);
        }
        v.down();
    }
    
    public String toString() {
        String ret = "parent: "+ parent.toString()+" children: ";
        
        Iterator<Tree<T>> i = children.iterator();
        while(i.hasNext()) {
            ret += i.next().toString();
        }
        return ret;
    }
    T parent;
    ArrayList<Tree<T>> children;
}
