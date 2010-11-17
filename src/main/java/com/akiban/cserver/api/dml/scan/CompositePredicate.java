// NOT FINISHED

//package com.akiban.cserver.api.dml.scan;
//
//import java.util.Arrays;
//import java.util.EnumSet;
//import java.util.Map;
//import java.util.Set;
//
//public final class CompositePredicate implements Predicate {
//    public static class CompositionException extends Exception {
//        private CompositionException(String message) {
//            super(message);
//        }
//    }
//
//    private NiceRow startRow = null;
//    private NiceRow endRow = null;
//    private final Set<ScanFlag> scanFlags = EnumSet.of(
//            ScanFlag.START_AT_BEGINNING,
//            ScanFlag.END_AT_END
//    );
//
//    public CompositePredicate(Predicate... predicates) throws CompositionException {
//        for (Predicate predicate : predicates) {
//            if (predicate == null) {
//                throw new IllegalArgumentException("no component predicates may be null: "
//                        + Arrays.asList(predicates));
//            }
//            addPredicate(predicate);
//        }
//    }
//
//    private void addPredicate(Predicate predicate) throws CompositionException {
//        if (predicate.getStartRow() == predicate.getEndRow()) {
//            if ( (startRow == null) && (endRow == null) ) {
//                startRow = predicate.getStartRow();
//                endRow = predicate.getEndRow();
//                return;
//            }
//            else if ( (startRow != endRow) || (startRow != predicate.getStartRow()) ) {
//                throw new CompositionException("Can't combine equality and non-equality predicates");
//            }
//        }
//
//        if (!predicate.getScanFlags().contains(ScanFlag.START_AT_BEGINNING)) {
//            addPredicateStartRow(predicate);
//        }
//        if (!predicate.getScanFlags().contains(ScanFlag.END_AT_END)) {
//            final NiceRow predicateEnd = predicate.getEndRow();
//            assert predicateEnd != null : predicate;
//            addPredicateEndRow(predicateEnd);
//        }
//    }
//
//    private void addPredicateEndRow(NiceRow endRow) {
//        assert endRow != null;
//    }
//
//    /**
//     * <p>Adds a lower bound to this predicate. The incoming predicate must have a lower bound (that is,
//     * its getStartRow() must not return null) and it must not be an equality (that is, its getStartRow()
//     * and getEndRow() must return different objects).</p>
//     *
//     * <p>If this predicate doesn't have a start row, one will be constructed to mimic the incoming predicate.
//     * Otherwise, each of the incoming predicate's start-row fields are compared to this predicate's start-row
//     * fields:
//     * </p>
//     *
//     * <ul>
//     * <li>If this predicate's startRow doesn't define the given field, it is added; this counts as an update.</li>
//     * <li>Otherwise, both fields must define values of the same class, and that class must
//     * implement Comparable</li>
//     * <li>If the incoming field value is less than or equal to this predicate's corresponding field's value,
//     * the incoming value is used; this counts as an update.</li>
//     * <li>If any of this predicate's fields are updated, and the incoming field has the START_RANGE_EXCLUSIVE
//     * flag set, that flag will also be set on this predicate</p>
//     * </ul>
//     *
//     * @param incoming the lower bound; see above for details
//     */
//    private void addPredicateStartRow(Predicate incoming) {
//        final NiceRow predicateStart = incoming.getStartRow();
//        assert predicateStart != null : incoming;
//        assert ! incoming.getScanFlags().contains(ScanFlag.START_AT_BEGINNING) : incoming.getScanFlags();
//        // At this point, we know that there is a lower bound. We also assume that the
//
//        if (startRow == null) {
//            startRow = new NiceRow();
//            startRow.putAll(predicateStart.getFields());
//            if (incoming.getScanFlags().contains(ScanFlag.START_RANGE_EXCLUSIVE)) {
//                scanFlags.add(ScanFlag.START_RANGE_EXCLUSIVE);
//            }
//            return;
//        }
//
//        for (Map.Entry<Integer,Object> entry : predicateStart.getFields().entrySet()) {
//            final int index = entry.getKey();
//            final Object thiers = entry.getValue();
//
//            final Object mine = startRow.get(index);
//            if (mine)
//        }
//    }
//
//    @Override
//    public NiceRow getStartRow() {
//        throw new UnsupportedOperationException(); // TODO
//    }
//
//    @Override
//    public NiceRow getEndRow() {
//        throw new UnsupportedOperationException(); // TODO
//    }
//
//    @Override
//    public EnumSet<ScanFlag> getScanFlags() {
//        throw new UnsupportedOperationException(); // TODO
//    }
//}
