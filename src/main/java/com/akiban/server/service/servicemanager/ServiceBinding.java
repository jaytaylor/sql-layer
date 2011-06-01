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

package com.akiban.server.service.servicemanager;

public interface ServiceBinding {
    String getInterfaceName();

    void setImplementingClass(String className);
    String getImplementingClassName();

    boolean isDirectlyRequired();
    void markDirectlyRequired();

    /**
     * <p>Returns whether the given object is a ServiceBinding whose interface name matches this instance's.</p>
     * <p>Equality <em>must</em> be based solely on interface name.</p>
     * @param obj the other instance
     * @return whether the other instance is a ServiceBinding with equivalent interface name
     */
    @Override
    public boolean equals(Object obj);

    /**
     * Returns this instance's hash code, which must be compatible with {@link ServiceBinding#equals(Object)}
     * @return a hash compatible with {@link ServiceBinding#equals(Object)}
     */
    @Override
    public int hashCode();
}
