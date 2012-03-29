/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.test.pt;

import com.akiban.server.test.ApiTestBase;
import com.akiban.util.tap.Tap;
import com.akiban.util.tap.TapReport;
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
        tapsRegexes = new ArrayList<String>();
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

    private static final Comparator<TapReport> REPORTS_BY_NAME = new Comparator<TapReport>() {
        @Override
        public int compare(TapReport o1, TapReport o2) {
            return o1.getName().compareTo(o2.getName());
        }
    };
}
