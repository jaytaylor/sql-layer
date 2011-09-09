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

package com.akiban.sql.optimizer.plan;

import java.util.Map;

/** Somewhat like Cloneable, except that a deep copy is implied and it's possible
 * to request that the same object not be cloned twice in the new tree by keeping
 * it in a map.
 */
public interface Duplicatable
{
    public Duplicatable duplicate();
    public Duplicatable duplicate(DuplicateMap map);
}
