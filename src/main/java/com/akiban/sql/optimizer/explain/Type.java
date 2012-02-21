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
 * <b>Type</b>
 * Reflects object's type/class
 * Can be useful for those formatters that want to format
 * scan_xyz_operators' explainer differently from insert_operator's explainer, 
 * or to format functions' differently from binary operators (such as +, -, LIKE, ILIKE, etc
 * 
 * <b>GeneralType</b>
 * Refects each class of Explainers
 * 
 */
public enum Type
{
    AGGREGATOR(GeneralType.AGGREGATOR),
    FUNCTION(GeneralType.EXPRESSION), 
    BINARY_OPERATOR(GeneralType.EXPRESSION),
    SCAN_OPERATOR(GeneralType.OPERATOR),
    LOOKUP_OPERATOR(GeneralType.OPERATOR),
    COUNT_OPERATOR(GeneralType.OPERATOR),
    DUI(GeneralType.OPERATOR), // delete/update/insert
    DINSTINC_PARTIAL(GeneralType.OPERATOR),
    FILTER_DEFAULT(GeneralType.OPERATOR),
    PRODUCT_NESTED(GeneralType.OPERATOR),
    IF_ELSE(GeneralType.OPERATOR),
    UNION_ALL(GeneralType.OPERATOR),
    SORT(GeneralType.OPERATOR),
    FILTER(GeneralType.OPERATOR),
    PROJECT(GeneralType.OPERATOR),
    SELECT_HKEY(GeneralType.OPERATOR),
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
