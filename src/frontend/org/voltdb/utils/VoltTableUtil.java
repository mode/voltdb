/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.utils;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Spliterator;
import java.util.TimeZone;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.voltcore.utils.Pair;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.common.Constants;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;

import au.com.bytecode.opencsv_voltpatches.CSVWriter;


/*
 * Utility methods for work with VoltTables.
 */
public class VoltTableUtil {

    /*
     * Ugly hack to allow SnapshotConverter which
     * shares this code with the server to specify it's own time zone.
     * You wouldn't want to convert to anything other then GMT if you want to get the data back into
     * Volt using the CSV loader because that relies on the server to coerce the date string
     * and the server only supports GMT.
     */
    public static TimeZone tz = VoltDB.VOLT_TIMEZONE;

    // VoltTable status code to indicate null dependency table. Joining SPI replies to fragment
    // task messages with this.
    public static byte NULL_DEPENDENCY_STATUS = -1;
    public static byte DUMMY_DEPENDENCY_STATUS = -2;
    private static final ThreadLocal<SimpleDateFormat> m_sdf = new ThreadLocal<SimpleDateFormat>() {
        @Override
        public SimpleDateFormat initialValue() {
            SimpleDateFormat sdf = new SimpleDateFormat(
                    Constants.ODBC_DATE_FORMAT_STRING);
            sdf.setTimeZone(tz);
            return sdf;
        }
    };

    public static void toCSVWriter(CSVWriter csv, VoltTable vt, List<VoltType> columnTypes) throws IOException {
        final SimpleDateFormat sdf = m_sdf.get();
        String[] fields = new String[vt.getColumnCount()];
        while (vt.advanceRow()) {
            for (int ii = 0; ii < vt.getColumnCount(); ii++) {
                final VoltType type = columnTypes.get(ii);
                if (type == VoltType.BIGINT
                        || type == VoltType.INTEGER
                        || type == VoltType.SMALLINT
                        || type == VoltType.TINYINT) {
                    final long value = vt.getLong(ii);
                    if (vt.wasNull()) {
                        fields[ii] = Constants.CSV_NULL;
                    } else {
                        fields[ii] = Long.toString(value);
                    }
                } else if (type == VoltType.FLOAT) {
                    final double value = vt.getDouble(ii);
                    if (vt.wasNull()) {
                        fields[ii] = Constants.CSV_NULL;
                    } else {
                        fields[ii] = Double.toString(value);
                    }
                } else if (type == VoltType.DECIMAL) {
                    final BigDecimal bd = vt.getDecimalAsBigDecimal(ii);
                    if (vt.wasNull()) {
                        fields[ii] = Constants.CSV_NULL;
                    } else {
                        fields[ii] = bd.toString();
                    }
                } else if (type == VoltType.STRING) {
                    final String str = vt.getString(ii);
                    if (vt.wasNull()) {
                        fields[ii] = Constants.CSV_NULL;
                    } else {
                        fields[ii] = str;
                    }
                } else if (type == VoltType.TIMESTAMP) {
                    final TimestampType timestamp = vt.getTimestampAsTimestamp(ii);
                    if (vt.wasNull()) {
                        fields[ii] = Constants.CSV_NULL;
                    } else {
                        fields[ii] = sdf.format(timestamp.asApproximateJavaDate());
                        fields[ii] += String.format("%03d", timestamp.getUSec());
                    }
                } else if (type == VoltType.VARBINARY) {
                   byte bytes[] = vt.getVarbinary(ii);
                   if (vt.wasNull()) {
                       fields[ii] = Constants.CSV_NULL;
                   } else {
                       fields[ii] = Encoder.hexEncode(bytes);
                   }
                }
                else if (type == VoltType.GEOGRAPHY_POINT) {
                    final GeographyPointValue pt = vt.getGeographyPointValue(ii);
                    if (vt.wasNull()) {
                        fields[ii] = Constants.CSV_NULL;
                    }
                    else {
                        fields[ii] = pt.toString();
                    }
                }
                else if (type == VoltType.GEOGRAPHY) {
                    final GeographyValue gv = vt.getGeographyValue(ii);
                    if (vt.wasNull()) {
                        fields[ii] = Constants.CSV_NULL;
                    }
                    else {
                        fields[ii] = gv.toString();
                    }
                }
            }
            csv.writeNext(fields);
        }
        csv.flush();
    }

    public static Pair<Integer,byte[]>  toCSV(
            VoltTable vt,
            char delimiter,
            char fullDelimiters[],
            int lastNumCharacters) throws IOException {
        ArrayList<VoltType> types = new ArrayList<VoltType>(vt.getColumnCount());
        for (int ii = 0; ii < vt.getColumnCount(); ii++) {
            types.add(vt.getColumnType(ii));
        }
        return toCSV(vt, types, delimiter, fullDelimiters, lastNumCharacters);
    }

    /*
     * Returns the number of characters generated and the csv data
     * in UTF-8 encoding.
     */
    public static Pair<Integer,byte[]> toCSV(
            VoltTable vt,
            ArrayList<VoltType> columns,
            char delimiter,
            char fullDelimiters[],
            int lastNumCharacters) throws IOException {
        StringWriter sw = new StringWriter((int)(lastNumCharacters * 1.2));
        CSVWriter writer;
        if (fullDelimiters != null) {
            writer = new CSVWriter(sw,
                    fullDelimiters[0], fullDelimiters[1], fullDelimiters[2], String.valueOf(fullDelimiters[3]));
        }
        else if (delimiter == ',')
            // CSV
            writer = new CSVWriter(sw, delimiter);
        else {
            // TSV
            writer = CSVWriter.getStrictTSVWriter(sw);
        }
        toCSVWriter(writer, vt, columns);
        String csvString = sw.toString();
        return Pair.of(csvString.length(), csvString.getBytes(com.google_voltpatches.common.base.Charsets.UTF_8));
    }

    /**
     * Utility to aggregate a list of tables sharing a schema. Common for
     * sysprocs to do this, to aggregate results.
     */
    public static VoltTable unionTables(Collection<VoltTable> operands) {
        VoltTable result = null;

        // Locate the first non-null table to get the schema
        for (VoltTable vt : operands) {
            if (vt != null) {
                VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[vt.getColumnCount()];
                for (int ii = 0; ii < vt.getColumnCount(); ii++) {
                    columns[ii] = new VoltTable.ColumnInfo(vt.getColumnName(ii),
                            vt.getColumnType(ii));
                }
                result = new VoltTable(columns);
                result.setStatusCode(vt.getStatusCode());
                break;
            }
        }

        if (result != null) {
            for (VoltTable vt : operands) {
                if (vt != null) {
                    vt.resetRowPosition();
                    while (vt.advanceRow()) {
                        result.add(vt);
                    }
                }
            }

            result.resetRowPosition();
        }

        return result;
    }

    /**
     * Extract a table's schema.
     * @param vt  input table with source schema
     * @return  schema as column info array
     */
    public static VoltTable.ColumnInfo[] extractTableSchema(VoltTable vt)
    {
        VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[vt.getColumnCount()];
        for (int ii = 0; ii < vt.getColumnCount(); ii++) {
            columns[ii] = new VoltTable.ColumnInfo(vt.getColumnName(ii),
                    vt.getColumnType(ii));
        }
        return columns;
    }

    /**
     * Return true if any string field in the table contains param s.
     */
    public static boolean tableContainsString(VoltTable t, String s, boolean caseSenstive) {
        if (t.getRowCount() == 0) return false;
        if (!caseSenstive) {
            s = s.toLowerCase();
        }

        VoltTableRow row = t.fetchRow(0);
        do {
            for (int i = 0; i < t.getColumnCount(); i++) {
                if (t.getColumnType(i) == VoltType.STRING) {
                    String value = row.getString(i);
                    if (value == null) continue;
                    if (!caseSenstive) {
                        value = value.toLowerCase();
                    }
                    if (value.contains(s)) {
                        return true;
                    }
                }
            }

        } while (row.advanceRow());

        return false;
    }

    /**
     * Get a VoltTableRow as an array of Objects of the right type
     */
    public static Object[] tableRowAsObjects(VoltTableRow row) {
        Object[] result = new Object[row.getColumnCount()];
        for (int i = 0; i < row.getColumnCount(); i++) {
            result[i] = row.get(i, row.getColumnType(i));
        }
        return result;
    }

    /**
     * Support class for Java8-style streaming of table rows.
     */
    private static class VoltTableSpliterator implements Spliterator<VoltTableRow> {
        VoltTableRow m_row;
        final int m_fence;

        VoltTableSpliterator(VoltTable table, int origin, int fence) {
            m_fence = fence;

            if (origin == fence) {
                m_row = null;
                return;
            }

            assert(origin < fence);
            m_row = table.fetchRow(origin);
        }

        @Override
        public boolean tryAdvance(Consumer<? super VoltTableRow> action) {
            if ((m_row != null) && (m_row.getActiveRowIndex() < m_fence)) {
                 action.accept(m_row);
                 m_row = m_row.cloneRow();
                 m_row.advanceRow();
                 return true;
             }
             else { // cannot advance
                 return false;
             }
        }

        @Override
        public Spliterator<VoltTableRow> trySplit() {
            // no splitting until we have thread safety
            return null;
        }

        @Override
        public long estimateSize() {
            if (m_row == null) return 0;
            else return m_fence - m_row.getActiveRowIndex();
        }

        @Override
        public int characteristics() {
            return ORDERED | SIZED | IMMUTABLE | SUBSIZED;
        }

    }

    /**
     * Not yet public API for VoltTable and Java 8 streams
     */
    public static Stream<VoltTableRow> stream(VoltTable table) {
        return StreamSupport.stream(new VoltTableSpliterator(table, 0, table.getRowCount()), false);
    }
}
