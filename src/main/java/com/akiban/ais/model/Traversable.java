
package com.akiban.ais.model;

public interface Traversable
{
    void traversePreOrder(Visitor visitor);
    void traversePostOrder(Visitor visitor);
}
