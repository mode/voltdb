package org.voltdb.regressionsuites;

import com.google_voltpatches.common.collect.ImmutableList;

import java.lang.Byte;
import java.lang.Exception;
import java.lang.Short;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoader;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestPercentileSuite extends RegressionSuite {
    private static final String UNSUPPORTED_COLUMNS_TABLE = "unsupported_column_types";
    private static final List<String> UNSUPPORTED_COLUMN_NAMES =
            ImmutableList.of("vb", "vc", "vb_inline", "vc_inline", "gg", "gp");

    private static final List<String> SUPPORTED_COLUMNS_TABLES = ImmutableList.of(
            "replicated_table", "partitioned_table");
    private static final List<String> SUPPORTED_COLUMN_NAMES =
            ImmutableList.of("bi", "ii", "si", "ti", "dd", "ff", "ts");

    private static final double DEFAULT_ACCURACY_GOAL = 0.0001 / 100; // 0.0001%

    private Client client;

    public TestPercentileSuite(String name) throws IOException {
        super(name);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = getClient();
    }

    public void testMedianCases() throws Exception {
        for (String tableName : SUPPORTED_COLUMNS_TABLES) {
            // Clear the table before we get started. Just in case.
            callProcedure("@AdHoc", "DELETE FROM " + tableName);

            // Insert some test data.
            callProcedure(tableName + ".Insert", 1, 1, 1, 1, 1, 1, 1, 1);
            callProcedure(tableName + ".Insert", 2, 2, 2, 2, 2, 2, 2, 2);
            callProcedure(tableName + ".Insert", 3, 3, 3, 3, 3, 3, 3, 3);
            callProcedure(tableName + ".Insert", 4, 4, 4, 4, 4, 4, 4, 4);

            VoltTable t = callProcedure("@AdHoc", "select median(ff) from " + tableName).getResults()[0];
            assertTrue(t.advanceRow());
            assertDoubleEquals(t, 0, 2.5, DEFAULT_ACCURACY_GOAL, "select median(ff) from " + tableName);

            callProcedure(tableName + ".Insert", 5, 10, 10, 10, 10, 10, 10, 10);
            t = callProcedure("@AdHoc", "select median(ff) from " + tableName).getResults()[0];
            assertTrue(t.advanceRow());
            assertDoubleEquals(t, 0, 3.0, DEFAULT_ACCURACY_GOAL, "select median(ff) from " + tableName);
        }
    }

    public void testUnsupportedDataTypes() throws Exception {
        for (String columnName : UNSUPPORTED_COLUMN_NAMES) {
            for (PercentileMethod percentileMethod : PercentileMethod.values()) {
                verifyStmtFails(client,
                        String.format("SELECT %s(%s) FROM %s", percentileMethod, columnName, UNSUPPORTED_COLUMNS_TABLE),
                        "incompatible data type in operation");
            }
        }
    }

    public void testNullResults() throws Exception {
        // Test all permutations of table types (partitioned and replicated), all
        // supported column types, and all percentile methods.
        for (String tableName : SUPPORTED_COLUMNS_TABLES) {
            // Clear the table before we get started. Just in case.
            callProcedure("@AdHoc", "DELETE FROM " + tableName);

            // Insert a row with all null values (excepting the primary key)
            callProcedure(tableName + ".Insert", 1, null, null, null, null, null, null, null);

            for (String columnName : SUPPORTED_COLUMN_NAMES) {
                for (PercentileMethod percentileMethod : PercentileMethod.values()) {
                    final VoltTable vt = callProcedure("@AdHoc",
                            String.format("select %s(%s) from %s", percentileMethod.methodName(), columnName, tableName)
                    ).getResults()[0];
                    assertTrue(vt.advanceRow());

                    // The value returned should always be null
                    final double value = vt.getDouble(0);
                    assertTrue(
                            String.format("method=%s; column=%s; table=%s",
                                    percentileMethod.methodName(), columnName, tableName),
                            vt.wasNull());
                }
            }
        }
    }

    public void testMixedNullResults() throws Exception {
        // Test all permutations of table types (partitioned and replicated), all
        // supported column types, and all percentile methods.
        for (String tableName : SUPPORTED_COLUMNS_TABLES) {
            // Clear the table before we get started. Just in case.
            callProcedure("@AdHoc", "DELETE FROM " + tableName);

            // Insert a row with all null values (excepting the primary key)
            callProcedure(tableName + ".Insert", 1, null, null, null, null, null, null, null);

            // Insert two rows with non-null values
            callProcedure(tableName + ".Insert", 2, 0, 0, 0, 0, 0, 0, 0);
            callProcedure(tableName + ".Insert", 3, 100, 100, 100, 100, 100, 100, 100);

            for (String columnName : SUPPORTED_COLUMN_NAMES) {
                for (PercentileMethod percentileMethod : PercentileMethod.values()) {
                    final VoltTable vt = callProcedure("@AdHoc",
                            String.format("select %s(%s) from %s", percentileMethod.methodName(), columnName, tableName)
                    ).getResults()[0];
                    assertTrue(vt.advanceRow());

                    // The value returned should always match the percentile
                    assertDoubleEquals(vt, 0, percentileMethod.getPercentile(), DEFAULT_ACCURACY_GOAL,
                            String.format("method=%s; column=%s; table=%s",
                                    percentileMethod.methodName(),
                                    columnName,
                                    tableName));
                }
            }
        }
    }

    public void testEmptyResults() throws Exception {
        // Test all permutations of table types (partitioned and replicated), all
        // supported column types, and all percentile methods.
        for (String tableName : SUPPORTED_COLUMNS_TABLES) {
            // Clear the table before we get started. Just in case.
            callProcedure("@AdHoc", "DELETE FROM " + tableName);

            for (String columnName : SUPPORTED_COLUMN_NAMES) {
                for (PercentileMethod percentileMethod : PercentileMethod.values()) {
                    final VoltTable vt = callProcedure("@AdHoc",
                            String.format("select %s(%s) from %s", percentileMethod.methodName(), columnName, tableName)
                    ).getResults()[0];
                    assertTrue(vt.advanceRow());

                    // The value returned should always be null
                    final double value = vt.getDouble(0);
                    assertTrue(
                            String.format("method=%s; column=%s; table=%s",
                                    percentileMethod.methodName(), columnName, tableName),
                            vt.wasNull());
                }
            }
        }
    }

    public void testAsTableAgg() throws Exception {
        // Test all permutations of table types (partitioned and replicated), all
        // supported column types, and all percentile methods.
        for (String tableName : SUPPORTED_COLUMNS_TABLES) {
            // Clear the table before we get started. Just in case.
            callProcedure("@AdHoc", "DELETE FROM " + tableName);

            // Insert 101 rows with incrementing values
            for (int i = 0; i <= 100; i++) {
                callProcedure(tableName + ".Insert", i, i, i, i, i, i, i, i);
            }

            for (PercentileMethod percentileMethod : PercentileMethod.values()) {
                final String sql = String.format("select %1$s(bi), %1$s(ii), %1$s(si), %1$s(ti), %1$s(dd), %1$s(ff), %1$s(ts) from %2$s", percentileMethod.methodName(), tableName);
                final VoltTable vt = callProcedure("@AdHoc", sql).getResults()[0];
                assertTrue(vt.advanceRow());

                final String message = "percentile = " + percentileMethod.getPercentile() + "; table = " + tableName;
                assertDoubleEquals(vt, 0, percentileMethod.getPercentile(), DEFAULT_ACCURACY_GOAL, message + "; column = bi"); // ff
                assertDoubleEquals(vt, 1, percentileMethod.getPercentile(), DEFAULT_ACCURACY_GOAL, message + "; column = ii"); // ff
                assertDoubleEquals(vt, 2, percentileMethod.getPercentile(), DEFAULT_ACCURACY_GOAL, message + "; column = si"); // ff
                assertDoubleEquals(vt, 3, percentileMethod.getPercentile(), DEFAULT_ACCURACY_GOAL, message + "; column = ti"); // ff
                assertDoubleEquals(vt, 4, percentileMethod.getPercentile(), DEFAULT_ACCURACY_GOAL, message + "; column = dd"); // ff
                assertDoubleEquals(vt, 5, percentileMethod.getPercentile(), DEFAULT_ACCURACY_GOAL, message + "; column = ff"); // ff
                assertDoubleEquals(vt, 6, percentileMethod.getPercentile(), DEFAULT_ACCURACY_GOAL, message + "; column = ts"); // ff
            }
        }
    }

    public void testLargeTable() throws Exception {
        // Test all permutations of table types (partitioned and replicated)
        for (String tableName : SUPPORTED_COLUMNS_TABLES) {
            // Clear the table before we get started. Just in case.
            callProcedure("@AdHoc", "DELETE FROM " + tableName);

            VoltBulkLoader loader = client.getNewBulkLoader(tableName, 10_000, null);

            // Insert 1,000,000 rows with incrementing values
            for (int i = 0; i <= 1_000_000; i++) {
                loader.insertRow(i, i, i, i, i % Short.MAX_VALUE, i % Byte.MAX_VALUE, i, i, i);
            }

            loader.drain();
            loader.close();

            // We just want to know that the query will succeed with such a large dataset
            callProcedure("@AdHoc", "select median(bi) from " + tableName);
        }
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestPercentileSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setRssLimit("90%");
        final String literalSchema =
                "CREATE TABLE replicated_table ( " +
                "pk integer primary key not null, " +
                "bi bigint, " +
                "ii integer, " +
                "si smallint, " +
                "ti tinyint, " +
                "dd decimal, " +
                "ff float, " +
                "ts timestamp " +
                ");" +
                "CREATE TABLE partitioned_table ( " +
                "pk integer primary key not null, " +
                "bi bigint, " +
                "ii integer, " +
                "si smallint, " +
                "ti tinyint, " +
                "dd decimal, " +
                "ff float, " +
                "ts timestamp " +
                "); " +
                "partition table partitioned_table on column pk;" +
                "CREATE TABLE " + UNSUPPORTED_COLUMNS_TABLE + " ( " +
                "vb varbinary(256), " +
                "vc varchar(256)," +
                "vb_inline varbinary(4), " +
                "vc_inline varchar(4), " +
                "gg geography," +
                "gp geography_point," +
                ");";
        try {
            project.addLiteralSchema(literalSchema);
        }
        catch (IOException e) {
            assertFalse(true);
        }
        boolean success;

        config = new LocalCluster("testApproxCountDistinctSuite-onesite.jar", 3, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        config = new LocalCluster("testApproxCountDistinctSuite-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }

    private ClientResponse callProcedure(String procedure, Object... values) throws Exception {
        final ClientResponse response = getClient().callProcedureWithTimeout(60_000, procedure, values);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        return response;
    }

    private void assertDoubleEquals(VoltTable table, int column, double expectedValue, double acceptableRange, String message) {
        double actualValue = table.getDouble(column);
        assertFalse(message, table.wasNull());

        double percentDifference = Math.abs(actualValue - expectedValue) / expectedValue;
        assertTrue(String.format("%s: %f is more than %f from %f", message, actualValue, acceptableRange, expectedValue),
                acceptableRange >= percentDifference);
    }

    public static enum PercentileMethod {
        MEDIAN(50.0),
        PERCENTILE_1(1.0),
        PERCENTILE_5(5.0),
        PERCENTILE_25(25.0),
        PERCENTILE_75(75.0),
        PERCENTILE_95(95.0),
        PERCENTILE_99(99.0);

        private final double percentile;

        private PercentileMethod(double percentile) {
            this.percentile = percentile;
        }

        public String methodName() {
            return name();
        }

        public double getPercentile() {
            return percentile;
        }
    }
}