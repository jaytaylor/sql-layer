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
 * Identify each attribute
 */
public enum Label
{
    // CHILD(REN) OPERATION(S)
   //--------------------------------------------------------------------------
    AGGREGATORS(Category.CHILD),
    INPUT_OPERATOR(Category.CHILD),
    OPERAND(Category.CHILD), // function operand, operands in general
    PROJECTION(Category.CHILD), // list of a expressions
    PREDICATE(Category.CHILD),
    EXPRESSIONS(Category.CHILD),
    BLOOM_FILTER(Category.CHILD),
    HIGH_COMPARAND(Category.CHILD),
    LOW_COMPARAND(Category.CHILD),
    EQUAL_COMPARAND(Category.CHILD),
    
    
    // COST
    //--------------------------------------------------------------------------
    COST(Category.COST),
    
    // DESCRIPTION (may or may not needed)
    //--------------------------------------------------------------------------
    POSITION(Category.DESCRIPTION),
    BINDING_POSITION(Category.DESCRIPTION),
    EXTRA_TAG(Category.DESCRIPTION), // extra info
    INFIX_REPRESENTATION(Category.DESCRIPTION),
    ASSOCIATIVE(Category.DESCRIPTION),
    INDEX(Category.DESCRIPTION),
    PIPELINE(Category.DESCRIPTION),
    DEPTH(Category.DESCRIPTION),
    
    // IDENTIFIER
    //--------------------------------------------------------------------------
    NAME(Category.IDENTIFIER),
    START_TABLE(Category.IDENTIFIER),
    STOP_TABLE(Category.IDENTIFIER),
    TABLE_SCHEMA(Category.IDENTIFIER),
    TABLE_NAME(Category.IDENTIFIER),
    TABLE_CORRELATION(Category.IDENTIFIER),
    COLUMN_NAME(Category.IDENTIFIER),
    INDEX_NAME(Category.IDENTIFIER),
    
    // OPTION
    //--------------------------------------------------------------------------
    INPUT_PRESERVATION(Category.OPTION),
    GROUPING_OPTION(Category.OPTION),
    FLATTEN_OPTION(Category.OPTION), // keep parent, etc 
    SORT_OPTION(Category.OPTION),
    SCAN_OPTION(Category.OPTION), // full/deep.shallow, etc
    LIMIT(Category.OPTION),
    PROJECT_OPTION(Category.OPTION), // has a table or not
    JOIN_OPTION(Category.OPTION), // INNER, LEFT, etc
    ORDERING(Category.OPTION), // ASC or DESC
    INDEX_KIND(Category.OPTION),
    INDEX_SPATIAL_DIMENSIONS(Category.OPTION),
    ORDER_EFFECTIVENESS(Category.OPTION),
    USED_COLUMNS(Category.OPTION),
    NUM_SKIP(Category.OPTION),
    NUM_COMPARE(Category.OPTION),
    SET_OPTION(Category.OPTION),
    PROCEDURE_CALLING_CONVENTION(Category.OPTION),
    PROCEDURE_IMPLEMENTATION(Category.OPTION),

    // TYPE DESCRIPTION
    //--------------------------------------------------------------------------
    INNER_TYPE(Category.TYPE_DESCRIPTION),
    PARENT_TYPE(Category.TYPE_DESCRIPTION),
    CHILD_TYPE(Category.TYPE_DESCRIPTION),
    LEFT_TYPE(Category.TYPE_DESCRIPTION),
    RIGHT_TYPE(Category.TYPE_DESCRIPTION),
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
