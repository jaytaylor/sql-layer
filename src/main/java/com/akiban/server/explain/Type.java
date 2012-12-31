/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.explain;

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
    LIMIT_OPERATOR(GeneralType.OPERATOR),
    NESTED_LOOPS(GeneralType.OPERATOR),
    IF_EMPTY(GeneralType.OPERATOR),
    UNION(GeneralType.OPERATOR),
    SORT(GeneralType.OPERATOR),
    FILTER(GeneralType.OPERATOR),
    PROJECT(GeneralType.OPERATOR),
    SELECT_HKEY(GeneralType.OPERATOR),
    AGGREGATE(GeneralType.OPERATOR),
    ORDERED(GeneralType.OPERATOR),
    BLOOM_FILTER(GeneralType.OPERATOR),
    
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
