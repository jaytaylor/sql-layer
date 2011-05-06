package com.akiban.ais.model.aisb2;

public interface NewAkibanJoinBuilder {
    NewAkibanJoinBuilder on(String childColumn, String parentColumn);
    NewAkibanJoinBuilder and(String childColumn, String parentColumn);
}
