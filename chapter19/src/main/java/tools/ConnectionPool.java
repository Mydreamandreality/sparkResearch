package tools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

/**
 * Created by 張燿峰
 * 静态资源池
 *
 * @author 孤
 * @date 2019/4/16
 * @Varsion 1.0
 */
public class ConnectionPool {

    private static LinkedList<Connection> connectionQueue;

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized static Connection getConnection() {
        try {
            if (connectionQueue == null) {
                connectionQueue = new LinkedList<>();
            }
            for (int i = 0; i < 10; i++) {
                Connection conn = DriverManager.getConnection("jdbc:mysql://spark1:3307/test", "root", "root");
                connectionQueue.push(conn);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connectionQueue.poll();
    }

    /**
     * return push connection
     *
     * @param conn this connection
     */
    public static void returnConnection(Connection conn) {
        connectionQueue.push(conn);
    }
}
