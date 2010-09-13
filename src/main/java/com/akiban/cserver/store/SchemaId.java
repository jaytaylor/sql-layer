package com.akiban.cserver.store;

import java.io.Serializable;

public final class SchemaId implements Serializable {
    private int generation;

    public SchemaId(int generation) {
        this.generation = generation;
    }

    public int getGeneration() {
        return generation;
    }

    public void incrementGeneration() {
        ++generation;
    }

    @Override
    public String toString() {
        return Integer.toString(generation);
    }
}
