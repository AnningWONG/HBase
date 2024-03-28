package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * ClassName: ConnectionTest
 * Package: com.atguigu.hbase
 * Description:
 *
 * @Author Wang Anning
 * @Create 2024/3/26 15:11
 * @Version 1.0
 */
public class ConnectionTest {
    public static void testConnection() throws Exception {
        Configuration conf = new Configuration();
        // 添加Zookeeper地址，通过Zookeeper获得HBase Master地址
        conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
        // 获取同步连接
        Connection connection = ConnectionFactory.createConnection(conf);
        System.out.println("同步连接：" + connection);
        connection.close();
        // 获取异步连接
        /*
        CompletableFuture<AsyncConnection> asyncConnection = ConnectionFactory.createAsyncConnection(conf);
        asyncConnection.get().close();;
        */
    }
}
