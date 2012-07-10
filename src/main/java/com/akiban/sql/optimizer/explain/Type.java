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
package com.akiban.sql.optimizer.explain;

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
    FUNCTION(GeneralType.EXPRESSION), 
    BINARY_OPERATOR(GeneralType.EXPRESSION),
    SUBQUERY(GeneralType.EXPRESSION),
        
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
    UNION_ALL(GeneralType.OPERATOR),
    SORT(GeneralType.OPERATOR),
    FILTER(GeneralType.OPERATOR),
    PROJECT(GeneralType.OPERATOR),
    SELECT_HKEY(GeneralType.OPERATOR),
    PHYSICAL_OPERATOR(GeneralType.OPERATOR), // could be broken down to scan_operator, sort operator, etc?
    ORDERED(GeneralType.OPERATOR),
    
    // ROWTYPE    
    //--------------------------------------------------------------------------
    ROWTYPE (GeneralType.ROWTYPE),
    
    // SCALAR 
    //--------------------------------------------------------------------------
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
        ROWTYPE // may not be needed?
    }
}
