package com.pingcap.tidb.workload.insurance;

import com.pingcap.tidb.workload.insurance.utils.DbUtil;
import com.pingcap.tidb.workload.insurance.utils.Pcg32;
import com.pingcap.tidb.workload.insurance.utils.RandStringGenerator;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class Workload {

    private static String host = "127.0.0.1";
    private static int port = 4000;
    private static String user = "root";
    private static String password = "";
    private static String dbName = "cssb";
    private static int thread = 50;
    private static int printSize = 10000;
    private static int fetchSize = 10000;
    private static long workloadSize = 10_000_000_000L;

    private static int existsPercent = 90;
    private static int selectPercent = 50;

    public static void main(String[] args) throws Exception {
        parseCommandLine(args);
        DbUtil.getInstance().initConnectionPool(String.format(
            "jdbc:mysql://%s:%s/%s?useunicode=true&characterEncoding=utf8&rewriteBatchedStatements=true&useLocalSessionState=true&cachePrepStmts=true&useServerPrepStmts=true",
            host, port, dbName), user, password);
        System.out.println(new Date() + " start workload....");
        queryIds();
        Workload.workload(thread);
    }


    private static class Record {

        private String customername;
        private String idtype;
        private String idcode;

        volatile boolean used = false;
    }

    private synchronized static Record getNextRecord(Pcg32 pcg, boolean modify) {
//        return  ids.poll();

        Record r = ids[pcg.nextInt(ids.length)];
        if (!modify) {
            return r;
        }
        while (r.used) {
            r = ids[pcg.nextInt(ids.length)];
        }
        r.used = true;
        return r;
    }

    private synchronized static void resetFlags(Record r) {
        r.used = false;
//        ids.add(r);
    }

    private static Record[] ids = null;
//    private static ArrayBlockingQueue<Record> ids = null;

    private static void queryIds() throws Exception {
        Connection conn = null;
        try {
            ids = new Record[fetchSize];
//            ids = new ArrayBlockingQueue<Record>(fetchSize);
            conn = DbUtil.getInstance().getConnection();
            System.out.println(new Date() + " start to query random record from TiDB....");
            PreparedStatement ps = conn.prepareStatement(String
                .format(
                    "select customername, idtype, idcode  from (select * from faceidentify limit 50000) t order by rand() limit %d; ",
                    fetchSize));
            ResultSet rs = ps.executeQuery();
            int index = 0;
            while (rs.next()) {
                Record record = new Record();
                record.customername = rs.getString(1);
                record.idtype = rs.getString(2);
                record.idcode = rs.getString(3);
//                ids.add(record);
                ids[index++] = record;
            }
            rs.close();
        } finally {
            if (conn != null) {
                DbUtil.getInstance().closeConnection(conn);
            }
        }
        System.out.println(new Date() + " query random record from TiDB done");
    }

    private static final String selectSQL = "select * from faceidentify where customername=? and idtype=? and idcode=?";
    private static final String updateSQL = "update faceidentify set identifyversion=?, identifycode=? where customername=? and idtype=? and idcode=?";

    private static void workload(int concurrency) {
        CountDownLatch tmpwg = new CountDownLatch(concurrency);
        for (int i = 0; i < concurrency; i++) {
            new Thread(() -> {
                Connection selectConnection = null;
                Connection updateConnection = null;
                try {
                    selectConnection = DbUtil.getInstance().getConnection();
                    updateConnection = DbUtil.getInstance().getConnection();
                    selectConnection.setAutoCommit(true);
                    updateConnection.setAutoCommit(false);
                    PreparedStatement selectPs = selectConnection.prepareStatement(selectSQL);
                    PreparedStatement updatePs = updateConnection.prepareStatement(updateSQL);
                    RandStringGenerator stringGenerator = new RandStringGenerator();

                    Pcg32 pcg = new Pcg32();
                    long threadFinishedSize = 0;
                    while (threadFinishedSize < workloadSize) {
                        try {
                            int actionModel = pcg.nextInt(100);
                            int model = pcg.nextInt(100);
                            if (actionModel <= selectPercent) {
                                if (model <= existsPercent) {
                                    Record id = getNextRecord(pcg, false);
                                    selectPs.setString(1, id.customername);
                                    selectPs.setString(2, id.idtype);
                                    selectPs.setString(3, id.idcode);
                                } else {
                                    selectPs.setString(1, "" + pcg.nextInt(10000));
                                    selectPs.setString(2, "" + pcg.nextInt(3));
                                    selectPs.setString(3, "" + pcg.nextInt(10000));
                                }
                                selectPs.executeQuery();
                            } else {
                                Record id = null;
                                if (model <= existsPercent) {
                                    id = getNextRecord(pcg, true);
                                    updatePs.setString(3, id.customername);
                                    updatePs.setString(4, id.idtype);
                                    updatePs.setString(5, id.idcode);
                                } else {
                                    updatePs.setString(3, "" + pcg.nextInt(10000));
                                    updatePs.setString(4, "" + pcg.nextInt(3));
                                    updatePs.setString(5, "" + pcg.nextInt(10000));
                                }
                                updatePs.setString(1, stringGenerator.genRandStr(50));
                                updatePs.setString(2, stringGenerator.genRandStr(7000));
                                updatePs.execute();
                                updateConnection.commit();
                                if (id != null) {
                                    resetFlags(id);
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            DbUtil.getInstance().closeConnection(selectConnection);
                            DbUtil.getInstance().closeConnection(updateConnection);
                            selectConnection = DbUtil.getInstance().getConnection();
                            updateConnection = DbUtil.getInstance().getConnection();
                            selectConnection.setAutoCommit(true);
                            updateConnection.setAutoCommit(false);
                            selectPs = selectConnection.prepareStatement(selectSQL);
                            updatePs = updateConnection.prepareStatement(updateSQL);
                        }
                        threadFinishedSize++;
                        if (threadFinishedSize % printSize == 0) {
                            System.out.println(
                                new Date() + " " + Thread.currentThread().getId() + " finished  "
                                    + threadFinishedSize + " workload, remain " + (workloadSize
                                    - threadFinishedSize));
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    tmpwg.countDown();
                    try {
                        if (selectConnection != null) {
                            DbUtil.getInstance().closeConnection(selectConnection);
                        }
                        if (updateConnection != null) {
                            DbUtil.getInstance().closeConnection(updateConnection);
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
        try {
            tmpwg.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void parseCommandLine(String[] args) throws Exception {
        Options opts = new Options();
        Option optHost = new Option("h", "host", true, "mysql host");
        Option optPort = new Option("P", "port", true, "mysql port");
        Option optUser = new Option("u", "user", true, "mysql user");
        Option optPassword = new Option("p", "password", true, "mysql password");
        Option optThread = new Option("t", "thread", true, "thread num");
        Option optDatabase = new Option("s", "database", true, "database name");
        Option optSize = new Option("f", "fetch", true,
            "fetch id size from db as the exists ids to point get or point update");
        Option optInfoSize = new Option("i", "print", true, "print a log per insert count");
        Option optHelp = new Option("v", "help", false, "print help message");

        Option optQueryPercent = new Option("q", "query-percent", true,
            "query percent , default value is 50");
        Option optExistsPercent = new Option("e", "exists-percent", true,
            "exists percent, default value is 90");

        opts.addOption(optHost);
        opts.addOption(optPort);
        opts.addOption(optUser);
        opts.addOption(optPassword);
        opts.addOption(optThread);
        opts.addOption(optDatabase);
        opts.addOption(optSize);
        opts.addOption(optInfoSize);
        opts.addOption(optHelp);
        opts.addOption(optQueryPercent);
        opts.addOption(optExistsPercent);
        opts.addOption("w", "workload-size", true, "query or update per thread");

        CommandLineParser parser = new DefaultParser();
        CommandLine line = parser.parse(opts, args);
        if (line.hasOption("h")) {
            host = line.getOptionValue("h");
        }
        if (line.hasOption("P")) {
            port = Integer.parseInt(line.getOptionValue("P"));
        }
        if (line.hasOption("u")) {
            user = line.getOptionValue("u");
        }
        if (line.hasOption("p")) {
            password = line.getOptionValue("p");
        }
        if (line.hasOption("t")) {
            thread = Integer.parseInt(line.getOptionValue("t"));
        }
        if (line.hasOption("s")) {
            dbName = line.getOptionValue("s");
        }
        if (line.hasOption("i")) {
            printSize = Integer.parseInt(line.getOptionValue("i"));
        }
        if (line.hasOption("f")) {
            fetchSize = Integer.parseInt(line.getOptionValue("f"));
        }
        if (line.hasOption("q")) {
            selectPercent = Integer.parseInt(line.getOptionValue("q"));
        }
        if (line.hasOption("e")) {
            existsPercent = Integer.parseInt(line.getOptionValue("e"));
        }
        if (line.hasOption("w")) {
            workloadSize = Long.parseLong(line.getOptionValue("w"));
        }
        if (line.hasOption("v")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("workload", opts, true);
            System.exit(0);
        }
    }
}
