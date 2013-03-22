
package com.akiban.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class MultipleCauseException extends RuntimeException {
    private final List<Throwable> causes = new ArrayList<>();

    public void addCause(Throwable cause) {
        causes.add(cause);
    }

    @Override
    public String toString() {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        printWriter.printf("%d cause%s%n", causes.size(), causes.size() == 1 ? ":" : "s:");
        for (Enumerated<Throwable> cause : EnumeratingIterator.of(causes)) {
            printWriter.printf("%d:%n----------------------------%n", cause.count());
            cause.get().printStackTrace(printWriter);
        }
        printWriter.flush();
        stringWriter.flush();
        return stringWriter.toString();
    }

    public List<Throwable> getCauses() {
        return Collections.unmodifiableList(causes);
    }
    
    public static RuntimeException combine(RuntimeException oldProblem, RuntimeException newProblem) {
        if (oldProblem == null)
            return newProblem;
        if (oldProblem instanceof MultipleCauseException) {
            MultipleCauseException mce = (MultipleCauseException) oldProblem;
            mce.addCause(newProblem);
            return mce;
        }
        MultipleCauseException mce = new MultipleCauseException();
        mce.addCause(oldProblem);
        mce.addCause(newProblem);
        return mce;
    }
}
