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

package com.akiban.util.tap;

public interface TapMXBean {

    public void enableAll();

    public void disableAll();

    public void setEnabled(final String regExPattern, final boolean on);

    public void setCustomTap(final String regExPattern, final String className)
            throws Exception;

    public void reset(final String regExPattern);

    public void resetAll();

    public String getReport();

    public TapReport[] getReports(final String regExPattern);

}
