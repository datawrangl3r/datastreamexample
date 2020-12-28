package com.mysqlcon;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;


public class Connector {

    private static char DELIMITER = ',';

    static void myMethod() {
        System.out.println("I just got executed!");
    }

    private static void writeHeader(final ResultSetMetaData rsmd,
        final int columnCount, final PrintWriter pw) throws SQLException {
        for (var i = 1; i <= columnCount; i++) {
            pw.write(rsmd.getColumnName(i));
            if (i != columnCount) {
                pw.append(DELIMITER);
            }
        }
        pw.println();
    }

    public static void main(String[] args) {

        String url = "jdbc:mysql://localhost:3306/classicmodels?allowPublicKeyRetrieval=true&useSSL=false";
        String user = "testuser";
        String password = "password";

        String query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = 'classicmodels'";

        ArrayList <String> myTableList = new ArrayList <String> ();

        try (Connection con = DriverManager.getConnection(url, user, password); Statement st = con.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
            java.sql.ResultSet.CONCUR_READ_ONLY);) {
            st.setFetchSize(10);
            ResultSet rs = st.executeQuery(query);
            while (rs.next()) {

                String tableName = rs.getString(1);
                myTableList.add(tableName);
            }

        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(Connector.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }

        System.out.println(myTableList);
        Iterator itr = myTableList.iterator();

        while (itr.hasNext()) {
            String tableName2 = itr.next().toString();
            String content = "SELECT * FROM " + tableName2;

            try (Connection con2 = DriverManager.getConnection(url, user, password); Statement st2 = con2.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
                java.sql.ResultSet.CONCUR_READ_ONLY);) {
                st2.setFetchSize(1000);
                ResultSet rs2 = st2.executeQuery(content);
                ResultSetMetaData rsmd = rs2.getMetaData();
                int columnCount = rsmd.getColumnCount();

                try {
                    OutputStream os = new FileOutputStream(tableName2 + ".csv");
                    var pw = new PrintWriter(os, true);
                    writeHeader(rsmd, columnCount, pw);
                    while (rs2.next()) {
                        for (var i = 1; i <= columnCount; i++) {
                            final
                            var value = rs2.getObject(i);
                            pw.write(value == null ? "" : value.toString());
                            if (i != columnCount) {
                                pw.append(DELIMITER);
                            }
                        }
                        pw.println();
                    }
                    pw.flush();
                } catch (FileNotFoundException ex) {
                    Logger lgr = Logger.getLogger(Connector.class.getName());
                    lgr.log(Level.SEVERE, ex.getMessage(), ex);
                }
            } catch (SQLException ex) {

                Logger lgr = Logger.getLogger(Connector.class.getName());
                lgr.log(Level.SEVERE, ex.getMessage(), ex);
            }
        }
    }
}