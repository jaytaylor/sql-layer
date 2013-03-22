
package com.akiban.server.store.statistics.histograms;

import java.util.List;

interface SampleVisitor<T> {
    void init();
    List<? extends T> visit(T sample);
    void finish();
}
