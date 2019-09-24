package com.pingcap.tidb.workload.insurance;

import com.pingcap.tidb.workload.insurance.utils.DbUtil;
import com.pingcap.tidb.workload.insurance.utils.RandStringGenerator;
import com.pingcap.tidb.workload.insurance.utils.UidGenerator;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class Main {

    private static String host = "127.0.0.1";
    private static int port = 4000;
    private static String user = "root";
    private static String password = "";
    private static String dbName = "cssb";
    private static int thread = 50;
    private static boolean dryRun = false;
    private static int startWorkId = 1;
    private static int batchSize = 200;
    private static long totalSize = 1_000_000_000L;
    private static int printSize = 10000;

    public static void main(String[] args) throws Exception {
        parseCommandLine(args);
        DbUtil.getInstance().initConnectionPool(String.format(
            "jdbc:mysql://%s:%s/%s?useunicode=true&characterEncoding=utf8&rewriteBatchedStatements=true&useLocalSessionState=true",
            host, port, dbName), user, password);
        System.out.println(new Date() + " start insert data...." );
        Main.workload(thread);
    }

    private static void workload(int concurrency) {
        CountDownLatch tmpwg = new CountDownLatch(concurrency);
        final AtomicInteger workId = new AtomicInteger(startWorkId);
        final AtomicLong remainSize = new AtomicLong(-totalSize);
        final long sizePerThread = totalSize / concurrency;
        for (int i = 0; i < concurrency; i++) {
            new Thread(() -> {
                Connection conn = null;
                try {
                    conn = DbUtil.getInstance().getConnection();
                    conn.setAutoCommit(false);
                    PreparedStatement inPstmt = conn.prepareStatement(insertSQL);
                    final UidGenerator uidGenerator = new UidGenerator(30, 20, 13);
                    uidGenerator.setWorkerId(workId.getAndAdd(1));
                    RandStringGenerator stringGenerator = new RandStringGenerator();
                    long threadInsertedSize = 0;
                    while (threadInsertedSize < sizePerThread) {
                        try {
                            insert(inPstmt, uidGenerator, stringGenerator);
                            conn.commit();
                        } catch (Exception e) {
                            e.printStackTrace();
                            DbUtil.getInstance().closeConnection(conn);
                            conn = DbUtil.getInstance().getConnection();
                            conn.setAutoCommit(false);
                            inPstmt = conn.prepareStatement(insertSQL);
                        }
                        threadInsertedSize += batchSize;
                        if (threadInsertedSize % printSize == 0) {
                            System.out.println(new Date() + " " + Thread.currentThread().getId() + "  add batch done: batch= " + batchSize + " thread total="
                                + threadInsertedSize + " remain size=" + (-remainSize.addAndGet(printSize)));
                        }

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    tmpwg.countDown();
                    try {
                        if (conn != null) {
                            DbUtil.getInstance().closeConnection(conn);
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

    private static final String insertSQL = "insert into faceidentify("
        + "customercode, customername, idtype, idcode, identifycode, identifyversion,"
        + " sex, createtime, amendtime, reserve1, reserve2) values "
        + "(?,?,?,?,?,?,"
        + "?,?,?,?,?)";

    private static void insert(PreparedStatement inPstmt, UidGenerator uidGenerator,
        RandStringGenerator stringGenerator)
        throws SQLException {
        long now = System.currentTimeMillis();
        for (int i = 0; i < batchSize; i++) {
            inPstmt.setString(1, uidGenerator.getUID() + "");
            inPstmt.setString(2, stringGenerator.genRandStr(50));
            inPstmt.setString(3, "01");
            inPstmt.setString(4, stringGenerator.genRandStr(50));
            inPstmt.setString(5, stringGenerator.genRandStr(7000));
            inPstmt.setString(6, stringGenerator.genRandStr(60));
            inPstmt.setInt(7, 1);
            inPstmt.setTimestamp(8, new Timestamp(now));
            inPstmt.setTimestamp(9, new Timestamp(now));
            inPstmt.setString(10, stringGenerator.genRandStr(40));
            inPstmt.setDouble(11, 4.5);
            inPstmt.addBatch();
        }
        if (!dryRun) {
            inPstmt.executeBatch();
        } else {
            inPstmt.clearBatch();
        }
    }


    private static void parseCommandLine(String[] args) throws Exception {
        Options opts = new Options();
        Option optHost = new Option("h", "host", true, "mysql host");
        Option optPort = new Option("P", "port", true, "mysql port");
        Option optUser = new Option("u", "user", true, "mysql user");
        Option optPassword = new Option("p", "password", true, "mysql password");
        Option optThread = new Option("t", "thread", true, "thread num");
        Option optDryRun = new Option("d", "dryRun", false, "dry run model");
        Option optDatabase = new Option("s", "database", true, "database name");
        Option optBatchSize = new Option("b", "batch", true, "batch size per insert");
        Option optWorkId = new Option("w", "work", true, "the start snow flake work node id, one work id per thread");
        Option optSize = new Option("c", "count", true, "total insert row count");
        Option optInfoSize = new Option("i", "print", true, "print a log per insert count");
        Option optHelp = new Option("v", "help", false, "print help message");

        opts.addOption(optHost);
        opts.addOption(optPort);
        opts.addOption(optUser);
        opts.addOption(optPassword);
        opts.addOption(optThread);
        opts.addOption(optDryRun);
        opts.addOption(optDatabase);
        opts.addOption(optBatchSize);
        opts.addOption(optWorkId);
        opts.addOption(optSize);
        opts.addOption(optInfoSize);
        opts.addOption(optHelp);

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
        if (line.hasOption("d")) {
            dryRun = true;
        }
        if (line.hasOption("s")) {
            dbName = line.getOptionValue("s");
        }
        if (line.hasOption("b")) {
            batchSize = Integer.parseInt(line.getOptionValue("b"));
        }
        if (line.hasOption("w")) {
            startWorkId = Integer.parseInt(line.getOptionValue("w"));
        }
        if (line.hasOption("c")) {
            totalSize = Long.parseLong(line.getOptionValue("c"));
        }
        if (line.hasOption("i")) {
            printSize = Integer.parseInt(line.getOptionValue("i"));
        }
        if(line.hasOption("v")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "workload", opts, true );
            System.exit(0);
        }
    }
}
