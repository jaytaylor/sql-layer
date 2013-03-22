
package com.akiban.sql.optimizer.rule.cost;

public interface CostModelMeasurements
{
    // From SelectCT
    final double SELECT_PER_ROW = 0.22;
    // From ProjectCT
    final double PROJECT_PER_ROW = 0.26;
    // From ExpressionCT
    final double EXPRESSION_PER_FIELD = 0.6;
    // From TreeScanCT
    final double RANDOM_ACCESS_PER_BYTE = 0.012;
    final double RANDOM_ACCESS_PER_ROW = 5.15;
    final double SEQUENTIAL_ACCESS_PER_BYTE = 0.0046;
    final double SEQUENTIAL_ACCESS_PER_ROW = 0.61;
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
}
