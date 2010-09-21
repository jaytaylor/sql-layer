package com.akiban.cserver.store;

import java.io.Serializable;

public final class SchemaId implements Serializable {

    private static final long serialVersionUID = -4442021857040031759L;
    
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SchemaId schemaId = (SchemaId) o;
        return generation == schemaId.generation;
    }

    @Override
    public int hashCode() {
        return generation;
    }

    @Override
    public String toString() {
        return String.format("SchemaId[%d]", generation);
    }
}
