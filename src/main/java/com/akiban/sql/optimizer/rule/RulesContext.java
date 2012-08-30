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

package com.akiban.sql.optimizer.rule;

import org.slf4j.Logger;

import java.util.List;
import java.util.Properties;

/** The context / owner of a {@link PlanContext}, shared among several of them. */
public class RulesContext
{
    // TODO: Need more much sophisticated invocation mechanism.
    private Properties properties;
    private List<? extends BaseRule> rules;

    protected RulesContext() {
    }

    protected void initProperties(Properties properties) {
        this.properties = properties;
    }

    protected void initRules(List<? extends BaseRule> rules) {
        this.rules = rules;
    }

    protected void initDone() {
        assert (properties != null) : "initProperties() not called";
        assert (rules != null) : "initRules() not called";
    }

    protected boolean rulesAre(List<? extends BaseRule> expected) {
        return rules == expected;
    }

    /** Make context with these rules. Just for testing. */
    public static RulesContext create(List<? extends BaseRule> rules,
                                      Properties properties) {
        RulesContext context = new RulesContext();
        context.initProperties(properties);
        context.initRules(rules);
        context.initDone();
        return context;
    }

    public void applyRules(PlanContext plan) {
        boolean logged = false;
        for (BaseRule rule : rules) {
            Logger logger = rule.getLogger();
            boolean debug = logger.isDebugEnabled();
            if (debug && !logged) {
                logger.debug("Before {}:\n{}", rule.getName(), plan.getPlan());
            }
            beginRule(rule);
            try {
                rule.apply(plan);
            }
            catch (RuntimeException e) {
                if (debug) {
                    String msg = "error while applying " + rule.getName() + " to " + plan.getPlan();
                    logger.debug(msg, e);
                }
                throw e;
            }
            finally {
                endRule(rule);
            }
            if (debug) {
                logger.debug("After {}:\n{}", rule.getName(), plan.getPlan());
            }
            logged = debug;
        }
    }

    /** Extend this to implement tracing, etc. */
    public void beginRule(BaseRule rule) {
    }
    public void endRule(BaseRule rule) {
    }

    /** Get optimizer configuration. */
    public Properties getProperties() {
        return properties;
    }
    public String getProperty(String key) {
        return properties.getProperty(key);
    }
    public String getProperty(String key, String defval) {
        return properties.getProperty(key, defval);
    }
}
