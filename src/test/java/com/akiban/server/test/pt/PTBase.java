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

package com.akiban.server.test.pt;

import com.akiban.server.test.ApiTestBase;
import com.akiban.util.Tap;
import com.akiban.util.TapReport;
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
        tapsRegexes = new ListTapsRegexes();
    }
    
    protected void relevantTaps(TapsRegexes tapsRegexes) {
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
        tapsRegexes.regexes.clear();
        relevantTaps(tapsRegexes);
        Tap.setEnabled(".*", true);
        Tap.reset(".*");
        Tap.defaultToOn(true);
    }

    @BeforeClass
    public static void createReportHeader() {
        log("params\ttest name\ttap name\tin\tout\ttime (ns)");
    }

    @After
    public void reportProfiling() throws Exception {
        // have to filter specifically, otherwise we'd have multiple TapReport[]s that we'd need to merge
        TapReport[] reportsArray = Tap.getReport(".*");
        List<TapReport> reports = new ArrayList<TapReport>(reportsArray.length);
        for (TapReport report : reportsArray) {
            String name = report.getName();
            boolean include = false;
            for (String regex : tapsRegexes.regexes) {
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

    private final ListTapsRegexes tapsRegexes;

    private static final Comparator<TapReport> REPORTS_BY_NAME = new Comparator<TapReport>() {
        @Override
        public int compare(TapReport o1, TapReport o2) {
            return o1.getName().compareTo(o2.getName());
        }
    };
    
    public interface TapsRegexes {
        void add(String regex);
    }
    
    private static final class ListTapsRegexes implements TapsRegexes {

        @Override
        public void add(String regex) {
            regexes.add(regex);
        }

        private final List<String> regexes = new ArrayList<String>();
    }
}
