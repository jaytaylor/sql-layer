package com.akiban.ais.model;

public interface Traversable
{
    void traversePreOrder(Visitor visitor) throws Exception;
    void traversePostOrder(Visitor visitor) throws Exception;
}
