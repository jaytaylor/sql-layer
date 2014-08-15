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

package com.foundationdb.sql.optimizer.rule.cost;

public interface CostModelMeasurements
{
    // From SelectCT
    final double SELECT_PER_ROW = 0.22;
    // From ProjectCT
    final double PROJECT_PER_ROW = 0.26;
    // From ExpressionCT
    final double EXPRESSION_PER_FIELD = 0.6;
    // From SortCT
    final double SORT_SETUP = 64;
    final double SORT_PER_ROW = 10;
    final double SORT_MIXED_MODE_FACTOR = 1.5;
    // From FlattenCT
    final double FLATTEN_OVERHEAD = 49;
    final double FLATTEN_PER_ROW = 41;
    // From MapCT
    final double MAP_PER_ROW = 0.15;
    // From ProductCT
    final double PRODUCT_PER_ROW = 40;
    // From SortWithLimitCT
    final double SORT_LIMIT_PER_ROW = 1;
    final double SORT_LIMIT_PER_FIELD_FACTOR = 0.2;
    // From DistinctCT
    final double DISTINCT_PER_ROW = 6;
    // From IntersectCT
    final double INTERSECT_PER_ROW = 0.25;
    // Also based on IntersectIT, since Union_Ordered works very similarly to Intersect_Ordered.
    final double UNION_PER_ROW = 0.25;
    // From HKeyUnionCT
    final double HKEY_UNION_PER_ROW = 2;
    // From Select_BloomFilterCT.
    final double BLOOM_FILTER_LOAD_PER_ROW = 0.24;
    final double BLOOM_FILTER_SCAN_PER_ROW = 0.39;
    final double BLOOM_FILTER_SCAN_SELECTIVITY_COEFFICIENT = 7.41;
    // From Select_HashTable
    final double HASH_TABLE_LOAD_PER_ROW = 0.24;
    final double HASH_TABLE_SCAN_PER_ROW = 0.39;
    final double HASH_TABLE_SCAN_SELECTIVITY_COEFFICIENT = 7.41;

}
