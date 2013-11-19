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

public interface PersistitCostModelMeasurements extends CostModelMeasurements
{
    // From TreeScanCT
    final double RANDOM_ACCESS_PER_BYTE = 0.012;
    final double RANDOM_ACCESS_PER_ROW = 5.15;
    final double SEQUENTIAL_ACCESS_PER_BYTE = 0.0046;
    final double SEQUENTIAL_ACCESS_PER_ROW = 0.61;
}
