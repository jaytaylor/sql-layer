/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.explain;

/**
 * <b>Type</b>
 * Reflects object's type/class
 * Can be useful for those formatters that want to format
 * scan_xyz_operator's explainer differently from insert_operator's explainer, 
 * or to format functions differently from binary operators (such as +, -, LIKE, ILIKE, etc)
 * 
 * <b>GeneralType</b>
 * Refects each class of Explainers
 * 
 */
public enum Type
{
    // AGGREGATOR
    //--------------------------------------------------------------------------
    AGGREGATOR(GeneralType.AGGREGATOR),
    
    // EXPRESSION
    //-------------------------------------------------------------------------- 
    FIELD(GeneralType.EXPRESSION),
    FUNCTION(GeneralType.EXPRESSION),
    BINARY_OPERATOR(GeneralType.EXPRESSION),
    SUBQUERY(GeneralType.EXPRESSION),
    LITERAL(GeneralType.EXPRESSION),
    VARIABLE(GeneralType.EXPRESSION),

    // OPERATORS
    //--------------------------------------------------------------------------
    SCAN_OPERATOR(GeneralType.OPERATOR),
    LOOKUP_OPERATOR(GeneralType.OPERATOR),
    COUNT_OPERATOR(GeneralType.OPERATOR),
    DUI(GeneralType.OPERATOR), // delete/update/insert
    DISTINCT(GeneralType.OPERATOR),
    FLATTEN_OPERATOR(GeneralType.OPERATOR),
    PRODUCT_OPERATOR(GeneralType.OPERATOR),
    LIMIT_OPERATOR(GeneralType.OPERATOR),
    NESTED_LOOPS(GeneralType.OPERATOR),
    IF_EMPTY(GeneralType.OPERATOR),
    UNION(GeneralType.OPERATOR),
    EXCEPT(GeneralType.OPERATOR),
    INTERSECT(GeneralType.OPERATOR),
    SORT(GeneralType.OPERATOR),
    FILTER(GeneralType.OPERATOR),
    PROJECT(GeneralType.OPERATOR),
    SELECT_HKEY(GeneralType.OPERATOR),
    AGGREGATE(GeneralType.OPERATOR),
    ORDERED(GeneralType.OPERATOR),
    BLOOM_FILTER(GeneralType.OPERATOR),
    BUFFER_OPERATOR(GeneralType.OPERATOR),
    HKEY_OPERATOR(GeneralType.OPERATOR),
    HASH_JOIN(GeneralType.OPERATOR),
    
    // PROCEDURE    
    //--------------------------------------------------------------------------
    PROCEDURE(GeneralType.PROCEDURE),
    
    // ROWTYPE    
    //--------------------------------------------------------------------------
    ROWTYPE(GeneralType.ROWTYPE),
    
    // ROW
    //--------------------------------------------------------------------------
    ROW(GeneralType.ROW),
    
    // SCALAR 
    //--------------------------------------------------------------------------
    FLOATING_POINT(GeneralType.SCALAR_VALUE),
    EXACT_NUMERIC(GeneralType.SCALAR_VALUE),
    STRING(GeneralType.SCALAR_VALUE),
    BOOLEAN(GeneralType.SCALAR_VALUE),
    
    // EXTRA_INFO
    //--------------------------------------------------------------------------
    EXTRA_INFO(GeneralType.EXTRA_INFO),
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
        PROCEDURE,
        SCALAR_VALUE,
        ROWTYPE,
        ROW,
        EXTRA_INFO
    }
}
