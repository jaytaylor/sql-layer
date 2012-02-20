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
    NAME(Category.IDENTIFIER),    
    COST(Category.COST),
    INPUT_OPERATOR(Category.CHILD),
    OPERAND(Category.CHILD), // function operand
    LOOK_UP_OPTION(Category.OPTION),
    GROUPING_OPTION(Category.OPTION),
    FLATTEN_OPTION(Category.OPTION),
    SORT_OPTION(Category.OPTION),
    PRESERVE_DUPLICAT(Category.OPTION),
    SCAN_OPTION(Category.OPTION), // full/deep.shallow, etc
    BINDING_POSITION(Category.DESCRIPTION),
    LIMIT(Category.OPTION),
    START_TABLE(Category.IDENTIFIER),
    STOP_TABLE(Category.IDENTIFIER),
    GROUP_TABLE(Category.IDENTIFIER),
    PROJECT_OPTION(Category.OPTION), // has a table or not
    JOIN_OPTION(Category.OPTION), // INNER, LEFT, etc
    ORDERING(Category.OPTION), // ASC or DESC
    INNER_TYPE(Category.TYPE_DESCRIPTION),
    OUTER_TYPE(Category.TYPE_DESCRIPTION),
    PRODUCT_TYPE(Category.TYPE_DESCRIPTION),
    OUTPUT_TYPE(Category.TYPE_DESCRIPTION),
    TABEL_TYPE(Category.TYPE_DESCRIPTION),
    ROWTYPE(Category.TYPE_DESCRIPTION),
    ANCESTOR_TYPE(Category.TYPE_DESCRIPTION),
    PROJECTIONS(Category.CHILD), // list of a expressions
    EXTRA_TAG(Category.DESCRIPTION), // extra info        
    ;
    

    public enum Category
    {
        IDENTIFIER,
        COST,
        OPTION,
        DESCRIPTION, //extra info (may not needed by the caller
        CHILD, // operand for expressions, or input operator for operator
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
