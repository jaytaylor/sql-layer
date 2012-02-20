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

package com.akiban.sql.optimizer.explain;

/**
 * Reflect object's type/class
 */
public enum Type
{
    AGGREGATOR(GeneralType.AGGREGATOR),
    FUNCTION(GeneralType.EXPRESSION), 
    BINARY_OPERATOR(GeneralType.EXPRESSION),
    PHYSICAL_OPERATOR(GeneralType.OPERATOR), // could be broken down to scan_operator, sort operator, etc?
    SUBQUERY(GeneralType.EXPRESSION),
    ROWTYPE (GeneralType.ROWTYPE),
    FLOATING_POINT(GeneralType.SCALAR_VALUE),
    EXACT_NUMERIC(GeneralType.SCALAR_VALUE),
    STRING(GeneralType.SCALAR_VALUE)
    ;
    
    private final GeneralType generalType;
    private Type (GeneralType type)
    {
        generalType = type;
    }
    
    public GeneralType generalType ()
    {
        return generalType;
    }
    
    public enum GeneralType
    {
        AGGREGATOR,
        EXPRESSION,
        OPERATOR,
        SCALAR_VALUE,
        ROWTYPE
    }
}
