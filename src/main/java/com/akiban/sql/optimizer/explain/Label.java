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
 * Identify each attribute
 */
public enum Label
{
    // CHILD(REN) OPERATION(S)
   //--------------------------------------------------------------------------
    AGGREGATORS(Category.CHILD),
    INPUT_OPERATOR(Category.CHILD),
    INNER_OPERATOR(Category.CHILD),
    OUTER_OPERATOR(Category.CHILD),
    OPERAND(Category.CHILD), // function operand, operands in general
    PROJECTION(Category.CHILD), // list of a expressions
    PREDICATE(Category.CHILD),
    LEFT(Category.CHILD),
    RIGHT(Category.CHILD),
    NUM_COMPARE(Category.CHILD),
    EXPRESSIONS(Category.CHILD),
    
    
    // COST
    //--------------------------------------------------------------------------
    COST(Category.COST),
    
    // DESCRIPTION (may or may not needed)
    //--------------------------------------------------------------------------
    BINDING_POSITION(Category.DESCRIPTION),
    EXTRA_TAG(Category.DESCRIPTION), // extra info
    INFIX_REPRESENTATION(Category.DESCRIPTION),
    ASSOCIATIVE(Category.DESCRIPTION),
    
    // IDENTIFIER
    //--------------------------------------------------------------------------
    NAME(Category.IDENTIFIER),
    START_TABLE(Category.IDENTIFIER),
    STOP_TABLE(Category.IDENTIFIER),
    GROUP_TABLE(Category.IDENTIFIER),
    
    // OPTION
    //--------------------------------------------------------------------------
    LOOK_UP_OPTION(Category.OPTION),
    GROUPING_OPTION(Category.OPTION),
    FLATTEN_OPTION(Category.OPTION), // keep parent, etc 
    SORT_OPTION(Category.OPTION),
    PRESERVE_DUPLICAT(Category.OPTION),
    SCAN_OPTION(Category.OPTION), // full/deep.shallow, etc
    LIMIT(Category.OPTION),
    PROJECT_OPTION(Category.OPTION), // has a table or not
    JOIN_OPTION(Category.OPTION), // INNER, LEFT, etc
    ORDERING(Category.OPTION), // ASC or DESC
    
    // TYPE DESCRIPTION
    //--------------------------------------------------------------------------
    INNER_TYPE(Category.TYPE_DESCRIPTION),
    PARENT_TYPE(Category.TYPE_DESCRIPTION),
    CHILD_TYPE(Category.TYPE_DESCRIPTION),
    KEEP_TYPE(Category.TYPE_DESCRIPTION),
    OUTER_TYPE(Category.TYPE_DESCRIPTION),
    PRODUCT_TYPE(Category.TYPE_DESCRIPTION),
    INPUT_TYPE(Category.TYPE_DESCRIPTION),
    OUTPUT_TYPE(Category.TYPE_DESCRIPTION),
    TABLE_TYPE(Category.TYPE_DESCRIPTION),
    ROWTYPE(Category.TYPE_DESCRIPTION),
    DINSTINCT_TYPE(Category.TYPE_DESCRIPTION),
    ANCESTOR_TYPE(Category.TYPE_DESCRIPTION),
    PREDICATE_ROWTYPE(Category.TYPE_DESCRIPTION),
    ;


    public enum Category
    {
        CHILD, // operand for expressions, or input operator for operator
        COST,
        DESCRIPTION, //extra info (may not needed by the caller        
        IDENTIFIER,        
        OPTION,        
        TYPE_DESCRIPTION,
    }

    public Category getCategory ()
    {
        return category;
    }

    private Label (Category g)
    {
        this.category = g;
    }

    private final Category category;
}
