
package com.akiban.sql;

import com.akiban.junit.Parameterization;

import org.junit.Ignore;

import java.util.Collection;
import java.util.ArrayList;

@Ignore
public class NamedParamsTestBase extends TestBase
{
    protected NamedParamsTestBase() {
    }

    protected NamedParamsTestBase(String caseName, String sql, String expected, String error) {
      super(caseName, sql, expected, error);
    }

    /** Given method args whose first one is caseName, make named parameterizations. */
    public static Collection<Parameterization> namedCases(Collection<Object[]> margs) {
        Collection<Parameterization> result = new ArrayList<>(margs.size());
        for (Object[] args : margs) {
            String caseName = (String)args[0];
            result.add(Parameterization.create(caseName, args));
        }
        return result;
    }

}
