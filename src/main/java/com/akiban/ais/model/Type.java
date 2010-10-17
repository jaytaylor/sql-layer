package com.akiban.ais.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Type implements Serializable, ModelNames
{
    @Override
    public String toString()
    {
        return "Type(" + name + ")";
    }

    public String name()
    {
        return name;
    }

    public Integer nTypeParameters()
    {
        return typeParameters;
    }

    public Boolean fixedSize()
    {
        return fixedSize;
    }

    public Long maxSizeBytes()
    {
        return maxStorageSizeBytes;
    }

    public String encoding()
    {
        return encoding;
    }

    @Override
    public boolean equals(Object object)
    {
        if (this == object) {
            return true;
        }
        if (!(object instanceof Type)) {
            return false;
        }
        Type type = (Type) object;
        return name.equals(type.name())
               && typeParameters.equals(type.nTypeParameters())
               && fixedSize.equals(type.fixedSize())
               && maxStorageSizeBytes.equals(type.maxSizeBytes())
               && encoding.equals(type.encoding());
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((typeParameters == null) ? 0 : typeParameters.hashCode());
        result = prime * result + ((fixedSize == null) ? 0 : fixedSize.hashCode());
        result = prime * result + ((maxStorageSizeBytes == null) ? 0 : maxStorageSizeBytes.hashCode());
        result = prime * result + ((encoding == null) ? 0 : encoding.hashCode());
        return result;
    }
    
    @SuppressWarnings("unused")
    private Type()
    {
        // GWT requires empty constructor
    }

    public Type(String name, Integer typeParameters, Boolean fixedSize, Long maxStorageSizeBytes, String encoding)
    {
        this.name = name;
        this.typeParameters = typeParameters;
        this.fixedSize = fixedSize;
        this.maxStorageSizeBytes = maxStorageSizeBytes;
        this.encoding = encoding;
    }

    public static void create(AkibaInformationSchema ais, Map<String, Object> map)
    {
        String name = (String) map.get(type_name);
        Integer typeParameters = (Integer) map.get(type_parameters);
        Boolean fixedSize = (Boolean) map.get(type_fixedSize);
        Long maxSizeBytes = (Long) map.get(type_maxSizeBytes);
        String encoding = (String) map.get(type_encoding);
        Type type = new Type(name, typeParameters, fixedSize, maxSizeBytes, encoding);
        ais.addType(type);
    }
    
    public Map<String, Object> map()
    {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(type_name, name);
        map.put(type_parameters, typeParameters);
        map.put(type_fixedSize, fixedSize);
        map.put(type_maxSizeBytes, maxStorageSizeBytes);
        map.put(type_encoding, encoding);
        return map;
    }

    private String name;
    private Integer typeParameters;
    private Boolean fixedSize;
    private Long maxStorageSizeBytes;
    private String encoding;
}
