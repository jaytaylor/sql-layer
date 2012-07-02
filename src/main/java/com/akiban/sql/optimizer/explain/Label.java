/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero Category Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero Category Public License for more details.
 *
 * You should have received a copy of the GNU Affero Category Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
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
    
    
    // COST
    //--------------------------------------------------------------------------
    COST(Category.COST),
    
    // DESCRIPTION (may or may not needed)
    //--------------------------------------------------------------------------
    BINDING_POSITION(Category.DESCRIPTION),
    EXTRA_TAG(Category.DESCRIPTION), // extra info
    
    // IDENTIFIER
    //--------------------------------------------------------------------------
    NAME(Category.IDENTIFIER),
    START_TABLE(Category.IDENTIFIER),
    STOP_TABLE(Category.IDENTIFIER),
    GROUP_TABLE(Category.IDENTIFIER),
    INFIX(Category.IDENTIFIER),
    
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
