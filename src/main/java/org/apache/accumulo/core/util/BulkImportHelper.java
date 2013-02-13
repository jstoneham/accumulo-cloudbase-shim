package org.apache.accumulo.core.util;

public class BulkImportHelper {
    public static class AssignmentStats {

        public final cloudbase.core.util.BulkImportHelper.AssignmentStats impl;

        public AssignmentStats(cloudbase.core.util.BulkImportHelper.AssignmentStats impl) {
            this.impl = impl;
        }

        public String toString() {
            return impl.toString();
        }
    }
}
