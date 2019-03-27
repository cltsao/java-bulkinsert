package com.example.bulkinsert.model;

/**
 * A record that can be inserted as a row through bulk insert, which reads the CSV string.
 */
public class DataRow {
    String[] fields;

    /**
     * SQLServerBulkCSVFileRecord reads an empty field as null.
     */
    protected void replaceNulls() {
        for (int i = 0; i < fields.length; ++i) {
            if (fields[i].equals("NULL"))
                fields[i] = "";
        }
    }

    public String toCsvString() {
        return String.join(",", fields);
    }
}
