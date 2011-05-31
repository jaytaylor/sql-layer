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

package com.akiban.server.manage;

@SuppressWarnings("unused") // used via JMX
public interface ManageMXBean {
    static final String MANAGE_BEAN_NAME = "com.akiban:type=Manage";

    void ping();

    int getJmxPort();

    boolean isDeferIndexesEnabled();

    void setDeferIndexes(final boolean defer);

    void buildIndexes(final String arg, final boolean deferIndexes);

    void deleteIndexes(final String arg);

    void flushIndexes();

    // TODO - temporary
    //
    String loadCustomQuery(String className, String path);

    String runCustomQuery(String params);
    
    String showCustomQueryResult();
    
    String stopCustomQuery();

    String getVersionString();
}
