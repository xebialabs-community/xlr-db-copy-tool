package com.xebialabs.release.tool;

import java.util.Properties;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.cli.*;

/**
 * 
 */
public class DBCopy {
    Connection srcConn;
    Connection tgtConn;
    String tgtDir;
    boolean isCsv = false;

    public DBCopy(CommandLine cmd) throws Exception {
        String optSource = cmd.getOptionValue("source");
        System.out.println("Source DB set to '" + optSource + "'");
        String optSourceUsername = cmd.getOptionValue("source_username");
        String optSourcePassword = cmd.getOptionValue("source_password");

        String optTarget = cmd.getOptionValue("target");
        System.out.println("Target set to '" + optTarget + "'");
        String optTargetUsername = cmd.getOptionValue("target_username");
        String optTargetPassword = cmd.getOptionValue("target_password");

        // create connections
        this.srcConn = DriverManager.getConnection(optSource, optSourceUsername, optSourcePassword);

        // target can be another database or a directory for CSV output
        if ( optTarget.startsWith("csv:")) {
            this.isCsv = true;
            // csv output directory
            this.tgtDir = optTarget.substring(4);
            // create the directory if necessary
            new File(this.tgtDir).mkdirs();
        } else if ( optTarget.startsWith("jdbc:")) {
            // database output
            this.tgtConn = DriverManager.getConnection(optTarget, optTargetUsername, optTargetPassword);
        } else {
            throw new Exception(String.format("Unknonw target type %s", optTarget));
        }
    }

    protected void execute() throws SQLException, IOException {
        // get source tables
        DatabaseMetaData metaData = this.srcConn.getMetaData();
        String[] types = { "TABLE" };

        // retrieving the columns in the database
        ResultSet tables = metaData.getTables(null, null, "%", types);
        while (tables.next()) {
            String tablename = tables.getString("TABLE_NAME");
            int rowcount = getRowCount(srcConn, tablename);
            System.out.println(String.format("Processing %s : %d rows", tablename, rowcount));

            if ( isCsv ) {
                String fname = String.format("%s/%s.csv", tgtDir, tablename);
                FileOutputStream fos = new FileOutputStream(fname);
                this.writeCsvRows(srcConn, tablename, fos);
                fos.close();
            }
        }
    }

    protected void close() throws SQLException {
        if ( this.srcConn != null ) this.srcConn.close();
        if ( this.tgtConn != null ) this.tgtConn.close();
    }
    
    protected void writeCsvRows(Connection conn, String tablename, OutputStream os) throws SQLException, IOException {
        String query = String.format("select * from %s", tablename);
        Statement statement = null;
        ResultSet rs = null;
        try {
            statement = conn.createStatement();
            rs = statement.executeQuery(query);

            ResultSetMetaData rsmd = rs.getMetaData();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                boolean first = true;
                for (int col = 1; col <= rsmd.getColumnCount(); col++) {
                    if (!first) sb.append(",");
                    sb.append(rs.getString(col));
                    first = false;
                }
                os.write(sb.toString().getBytes());
            }
        } finally {
            if (rs != null)
                rs.close();
            if (statement != null)
                statement.close();
        }
    }

    protected int getRowCount(Connection conn, String tablename) throws SQLException {
        String query = String.format("SELECT COUNT(*) AS recordCount FROM %s", tablename);
        Statement statement = null;
        ResultSet rs = null;
        int count = 0;
        try {
            statement = conn.createStatement();
            rs = statement.executeQuery(query);
            rs.next();
            count = rs.getInt("recordCount");
    
        } finally {
            if (rs != null)
                rs.close();
            if (statement != null)
                statement.close();
        }
        return count;        
    }

    // PUBLIC STATIC METHODS ==================================================

    public static void main(String[] args) throws Exception {
        // define command-line options
        Options options = new Options();
        options.addOption(new Option("s", "source", true, "JDBC connection string to source DB"));
        options.addOption(new Option("", "source_username", true, "Username for source DB"));
        options.addOption(new Option("", "source_password", true, "Password for source DB"));
        options.addOption(new Option("t", "target", true, "JDBC connection string to target DB"));
        options.addOption(new Option("", "target_username", true, "Username for target DB"));
        options.addOption(new Option("", "target_password", true, "Password for target DB"));

        // define parser
        CommandLineParser parser = new BasicParser();

        CommandLine cmd = parser.parse(options, args);

        DBCopy dbcopy = new DBCopy(cmd);
        try {
            dbcopy.execute();
        }
        finally {
            dbcopy.close();
        }
    }
}
