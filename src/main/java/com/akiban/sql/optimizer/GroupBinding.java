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

package com.akiban.sql.optimizer;

import com.akiban.ais.model.Group;

/**
 * A group binding: joins several TableBindings together.
 */
public class GroupBinding 
{
  private Group group;
  private String correlationName;
    
  public GroupBinding(Group group, String correlationName) {
    this.group = group;
    this.correlationName = correlationName;
  }

  public Group getGroup() {
    return group;
  }

  public String getCorrelationName() {
    return correlationName;
  }

  public String toString() {
    return group.toString() + " AS " + correlationName;
  }
}
