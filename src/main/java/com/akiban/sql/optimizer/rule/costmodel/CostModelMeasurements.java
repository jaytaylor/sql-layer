/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
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
    final double SELECT_PER_ROW = 1.6;
    // From ProjectCT
    final double PROJECT_PER_ROW = 0.15;
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
    final double FLATTEN_LEFT_JOIN_NO_CHILDREN = 417;
    final double FLATTEN_RIGHT_JOIN_NO_CHILDREN = 150;
    final double FLATTEN_LEFT_JOIN_OVERHEAD = 54;
    final double FLATTEN_RIGHT_JOIN_OVERHEAD = 120;
    final double FLATTEN_PER_ROW = 27;
}
