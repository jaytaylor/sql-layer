/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.ais.model;

import com.akiban.server.types.AkType;

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

    public AkType akType() {
        return akType;
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
        this(name, typeParameters, fixedSize, maxStorageSizeBytes, encoding, AkType.UNSUPPORTED);
    }

    public Type(String name, Integer typeParameters, Boolean fixedSize, Long maxStorageSizeBytes, String encoding, AkType akType)
    {
        this.name = name;
        this.typeParameters = typeParameters;
        this.fixedSize = fixedSize;
        this.maxStorageSizeBytes = maxStorageSizeBytes;
        this.encoding = encoding;
        this.akType = akType;
    }

    public static void create(AkibanInformationSchema ais, Map<String, Object> map)
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
    private AkType akType;
}
