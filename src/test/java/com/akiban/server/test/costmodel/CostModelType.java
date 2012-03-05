package com.akiban.server.test.costmodel;

public class CostModelType
{
    public String name()
    {
        return name;
    }
    
    private CostModelType(String name)
    {
        this.name = name;
    }
    
    public static final CostModelType INT = new CostModelType("int");
    public static final CostModelType VARCHAR = new CostModelType("varchar");

    private final String name;
}
