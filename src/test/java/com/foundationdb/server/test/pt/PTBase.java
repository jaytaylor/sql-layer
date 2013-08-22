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

package com.foundationdb.server.test.pt;

import com.foundationdb.server.test.ApiTestBase;
import com.foundationdb.util.tap.Tap;
import com.foundationdb.util.tap.TapReport;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public abstract class PTBase extends ApiTestBase {

    protected PTBase() {
        super("PT");
        tapsRegexes = new ArrayList<>();
    }
    
    protected void registerTaps() {
        // none by default
    }
    
    protected void beforeProfiling() throws Exception {
        // none by default
    }
    
    protected void afterProfileReporting() throws Exception {
        // none by default
    }
    
    protected static void log(String format, Object... args) {
        System.out.println(String.format(format, args));
    }
    
    protected String paramName() {
        return "";
    }
    
    @Before
    public void setUpProfiling() throws Exception {
        beforeProfiling();
        tapsRegexes.clear();
        registerTaps();
        Tap.setEnabled(".*", true);
        Tap.reset(".*");
        Tap.defaultToOn(true);
        testStartMS = System.currentTimeMillis();
    }

    @BeforeClass
    public static void createReportHeader() {
        log("params\ttest name\ttap name\tin\tout\ttime (ns)");
    }

    @After
    public void reportProfiling() throws Exception {
        testEndMS = System.currentTimeMillis();
        log("Total elapsed: %dms", testEndMS - testStartMS);
        // have to filter specifically, otherwise we'd have multiple TapReport[]s that we'd need to merge
        TapReport[] reportsArray = Tap.getReport(".*");
        List<TapReport> reports = new ArrayList<>(reportsArray.length);
        for (TapReport report : reportsArray) {
            String name = report.getName();
            boolean include = false;
            for (String regex : tapsRegexes) {
                if (name.matches(regex)) {
                    include = true;
                    break;
                }
            }
            if (include)
                reports.add(report);
        }
        Collections.sort(reports, REPORTS_BY_NAME);
        if (!reports.isEmpty()) {
            for (TapReport report : reports) {
                log("%s\t%s\t%s\t%d\t%d\t%d",
                        paramName().replace('\t', '_'),
                        testName(),
                        report.getName(),
                        report.getInCount(),
                        report.getOutCount(),
                        report.getCumulativeTime()
                );
            }
        }
        log("");
        afterProfileReporting();
    }

    protected final List<String> tapsRegexes;
    protected long testStartMS;
    protected long testEndMS;

    private static final Comparator<TapReport> REPORTS_BY_NAME = new Comparator<TapReport>() {
        @Override
        public int compare(TapReport o1, TapReport o2) {
            return o1.getName().compareTo(o2.getName());
        }
    };
}
