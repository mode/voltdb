package org.voltdb.regressionsuites;

import java.lang.Exception;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestPercentileSuite extends RegressionSuite {
    public TestPercentileSuite(String name) {
        super(name);
    }

    public void testAsTableAgg() throws Exception {
        Client client = getClient();

        ClientResponse med_cr = client.callProcedure("@AdHoc", "SELECT MEDIAN(bi) AS int_col, MEDIAN(dd) AS double_col FROM p;");
        assertEquals(med_cr.getStatus(), ClientResponse.SUCCESS);
        assertTrue(med_cr.getResults()[0].advanceRow());
        assertEquals(50.0, med_cr.getResults()[0].getDouble("int_col"));
        assertEquals(50.0, med_cr.getResults()[0].getDouble("double_col"));

        ClientResponse p1_cr = client.callProcedure("@AdHoc", "SELECT PERCENTILE_1(bi) AS int_col, PERCENTILE_1(dd) AS double_col FROM p;");
        assertEquals(p1_cr.getStatus(), ClientResponse.SUCCESS);
        assertTrue(p1_cr.getResults()[0].advanceRow());
        assertEquals(1.0, p1_cr.getResults()[0].getDouble("int_col"));
        assertEquals(1.0, p1_cr.getResults()[0].getDouble("double_col"));

        ClientResponse p5_cr = client.callProcedure("@AdHoc", "SELECT PERCENTILE_5(bi) AS int_col, PERCENTILE_5(dd) AS double_col FROM p;");
        assertEquals(p5_cr.getStatus(), ClientResponse.SUCCESS);
        assertTrue(p5_cr.getResults()[0].advanceRow());
        assertEquals(5.0, p5_cr.getResults()[0].getDouble("int_col"));
        assertEquals(5.0, p5_cr.getResults()[0].getDouble("double_col"));

        ClientResponse p25_cr = client.callProcedure("@AdHoc", "SELECT PERCENTILE_25(bi) AS int_col, PERCENTILE_25(dd) AS double_col FROM p;");
        assertEquals(p25_cr.getStatus(), ClientResponse.SUCCESS);
        assertTrue(p25_cr.getResults()[0].advanceRow());
        assertEquals(25.0, p25_cr.getResults()[0].getDouble("int_col"));
        assertEquals(25.0, p25_cr.getResults()[0].getDouble("double_col"));

        ClientResponse p75_cr = client.callProcedure("@AdHoc", "SELECT PERCENTILE_75(bi) AS int_col, PERCENTILE_75(dd) AS double_col FROM p;");
        assertEquals(p75_cr.getStatus(), ClientResponse.SUCCESS);
        assertTrue(p75_cr.getResults()[0].advanceRow());
        assertEquals(75.0, p75_cr.getResults()[0].getDouble("int_col"));
        assertEquals(75.0, p75_cr.getResults()[0].getDouble("double_col"));

        ClientResponse p95_cr = client.callProcedure("@AdHoc", "SELECT PERCENTILE_95(bi) AS int_col, PERCENTILE_95(dd) AS double_col FROM p;");
        assertEquals(p95_cr.getStatus(), ClientResponse.SUCCESS);
        assertTrue(p95_cr.getResults()[0].advanceRow());
        assertEquals(95.0, p95_cr.getResults()[0].getDouble("int_col"));
        assertEquals(95.0, p95_cr.getResults()[0].getDouble("double_col"));

        ClientResponse p99_cr = client.callProcedure("@AdHoc", "SELECT PERCENTILE_99(bi) AS int_col, PERCENTILE_99(dd) AS double_col FROM p;");
        assertEquals(p99_cr.getStatus(), ClientResponse.SUCCESS);
        assertTrue(p99_cr.getResults()[0].advanceRow());
        assertEquals(99.0, p99_cr.getResults()[0].getDouble("int_col"));
        assertEquals(99.0, p99_cr.getResults()[0].getDouble("double_col"));
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestPercentileSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE r ( " +
                "pk integer primary key not null, " +
                "bi bigint, " +
                "ii integer, " +
                "si smallint, " +
                "ti tinyint, " +
                "dd decimal, " +
                "ts timestamp " +
                ");" +
                "CREATE TABLE p ( " +
                "pk integer primary key not null, " +
                "bi bigint, " +
                "ii integer, " +
                "si smallint, " +
                "ti tinyint, " +
                "dd decimal, " +
                "ts timestamp " +
                "); " +
                "partition table p on column pk;" +
                "CREATE TABLE unsupported_column_types ( " +
                "vb varbinary(256), " +
                "vc varchar(256)," +
                "vb_inline varbinary(4), " +
                "vc_inline varchar(4), " +
                "ff float " +
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
}