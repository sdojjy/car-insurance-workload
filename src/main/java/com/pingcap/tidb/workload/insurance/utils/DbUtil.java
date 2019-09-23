package com.pingcap.tidb.workload.insurance.utils;

import java.sql.Connection;
import java.sql.SQLException;
import org.apache.commons.dbcp2.BasicDataSource;

public class DbUtil {

    private BasicDataSource bds;
    private String url;
    private String username;
    private String password;

    private static final DbUtil INSTANCE = new DbUtil();

    private DbUtil() {

    }

    public static DbUtil getInstance() {
        return INSTANCE;
    }

    public void initConnectionPool(String url, String username, String password) {
        bds = new BasicDataSource();
        bds.setUrl(url);
        bds.setUsername(username);
        bds.setPassword(password);
        bds.setInitialSize(3);
        bds.setMaxWaitMillis(3000);
        bds.setMaxIdle(20);
        bds.setMaxTotal(-1);
    }

    public Connection getConnection() throws SQLException {
        return bds.getConnection();
    }

    public void closeConnection(Connection conn) throws SQLException {
        try {
            conn.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
