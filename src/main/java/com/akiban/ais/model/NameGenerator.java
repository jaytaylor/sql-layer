package com.akiban.ais.model;

public interface NameGenerator
{
    String generateColumnName(Column column);
    String generateGroupIndexName(Index userTableIndex);
}
