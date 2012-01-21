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

public class TapMXBeanImpl implements TapMXBean {

    @Override
    public String getReport() {
        return Tap.report();
    }

    @Override
    public void reset(String regExPattern) {
        Tap.reset(regExPattern);
    }

    @Override
    public void resetAll() {
        Tap.reset(".*");
    }

    @Override
    public void disableAll() {
        Tap.setEnabled(".*", false);
    }

    @Override
    public void enableAll() {
        Tap.setEnabled(".*", true);
    }

    @Override
    public void setEnabled(final String regExPattern, final boolean on) {
        Tap.setEnabled(regExPattern, on);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setCustomTap(final String regExPattern, final String className)
            throws Exception {
        final Class<?> clazz = (Class.forName(className));
        if (Tap.class.isAssignableFrom(clazz)) {
            Class<? extends Tap> c = (Class<? extends Tap>) clazz;
            Tap.setCustomTap(regExPattern, c);
        } else {
            throw new ClassCastException(className + " is not a "
                    + Tap.class.getName());
        }
    }

    @Override
    public TapReport[] getReports(final String regExPattern) {
        return Tap.getReport(regExPattern);
    }
}
