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

package com.akiban.sql.optimizer.rule.costmodel;

import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    // From HKeyUnionCT
    final double HKEY_UNION_PER_ROW = 2;
    // From Select_BloomFilterCT.
    final double BLOOM_FILTER_LOAD_PER_ROW = 0.24;
    final double BLOOM_FILTER_SCAN_PER_ROW = 0.39;
    final double BLOOM_FILTER_SCAN_SELECTIVITY_COEFFICIENT = 7.41;
}
