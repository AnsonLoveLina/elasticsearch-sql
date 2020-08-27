package org.nlpcn.es4sql;

import org.elasticsearch.jdbc.ElasticSearchArray;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by zy-xx on 2019/9/18.
 */
public class TestJDBC {

    private static final String my_index_relation = "my_index_relation";

    private static final String my_index = "my_index";

    private static String query1 = "SELECT * from  " + my_index + " limit 0,10";

    private static String query2 = "SELECT * from  " + my_index_relation + " limit 0,10";

    private static String group1 = "SELECT stats(key2) from  " + my_index + " group by (key2,key3)";

    private static String group2 = "SELECT stats(key2) from  " + my_index + " ";

    private static String group3 = "SELECT stats(key2) from  " + my_index + " group by (key2),(key3)";

    private static String insertSql1 = "insert into " + my_index + " values(true,'1990-01-01 12:11:11',11.1,'41.12,-71.34',1,'是的',123213,'中华人民共和国')";

    private static String insertSql2 = "insert into " + my_index + "(fieldDate,fieldKeyword,fieldText) values('1990-01-01 12:11:11','是的','中华人民共和国')";

    private static String insertSql3 = "insert into " + my_index + "(fieldDate,fieldText) values('1990-01-01','中华人民共和国')";

    private static String insertSql4 = "insert into " + my_index + "(fieldDate,fieldText) values('1990-01-01 12','中华人民共和国')";

    static String insertSql5 = "INSERT INTO t_dsmanager_datafff (DATACODE, DATAROLELEVEL, ID, DATAISRELATION) VALUES ( ?,  ?,  ?,  ?)";

    private static String insertSql6 = "INSERT INTO t_dsmanager_datafff (DATACODE, DATAROLELEVEL, ID, DATAISRELATION) VALUES ( ?,  ?,  ?,  ?)";

    private static String insertSql7 = "insert into " + my_index + "(fieldBoolean,fieldDate,fieldDouble,fieldGeoPoin,fieldInteger,fieldKeyword,fieldLong,fieldText) values(true,'1990-01-01',11.1,'41.12,-71.34',1,'是的',123213,'中华人民共和国')";

    private static String updateSql1 = "update " + my_index + " set fieldDate='1990-01-01 12',fieldText='中华人民共和国'";

    private static String updateSql2 = "update " + my_index + " set fieldDate='1990-01-01 12',fieldText='中华人民共和国' where fieldKeyword='是的'";

    private static String updateSql3 = "update " + my_index + " set fieldDate='1990-01-01 12',fieldText='中华人民共和国' where q=query('中华')";

    private static String updateSql4 = "update " + my_index + " set fieldBoolean=false,fieldDate='1990-01-01 12',fieldDouble=1.11,fieldGeoPoin='41.12,-71.34',fieldInteger=2,fieldKeyword='是的',fieldLong=2320909,fieldText='中华人民共和国'";

    private static String user = "elastic";
    private static String password = "xx198742";
    private static String param = "";
    private static String param1 = "";
    private static java.util.Properties info = new java.util.Properties();

    static {
        param1 = "xpack.security.user=" + user + ":" + password;
        param = "xpack.security.user=" + user + ":" + password + "&xpack.security.transport.ssl.enabled=true&xpack.security.transport.ssl.verification_mode=certificate&xpack.security.transport.ssl.keystore.path=/Users/zy-xx/Documents/学习/elasticSearch/6/elasticsearch/elastic-certificates.p12&xpack.security.transport.ssl.truststore.path=/Users/zy-xx/Documents/学习/elasticSearch/6/elasticsearch/elastic-certificates.p12";
        info.put("xpack.security.user", user + ":" + password);
        info.put("xpack.security.transport.ssl.enabled", "true");
        info.put("xpack.security.transport.ssl.verification_mode", "certificate");
        info.put("xpack.security.transport.ssl.keystore.path", "/Users/zy-xx/elasticsearch.keystore");
        info.put("xpack.security.transport.ssl.truststore.path", "/Users/zy-xx/elasticsearch.keystore");
    }

    @org.junit.Test
    public void test() throws Exception {

//        System.out.println("sql" + insertSql5 + ":\n----------\n" + Util.sqlToEsQuery(query1));
        testInsertStatement();
        testInsertPrepareStatement();
        testUpdatePrepareStatement();
        testQueryStatement();
        testQueryPrepareStatement();
    }

    @org.junit.Test
    public void testUpdatePrepareStatement() throws Exception {
        Class.forName("org.elasticsearch.jdbc.ElasticSearchDriver");
        Connection connection = DriverManager.getConnection("jdbc:elasticsearch://127.0.0.1:9300?" + param);
//        Connection connection = DriverManager.getConnection("jdbc:elasticsearch://localhost:9300","elastic","changeme");
        connection.setAutoCommit(false);
        PreparedStatement ps = connection.prepareStatement("update /*! CONFLICTS(proceed) */t_dsmanager_data set DATACODE=?,ID=?");
        ps.setString(1, "qq");
        ps.setString(2, "ww");
        int result = ps.executeUpdate();
        connection.commit();
//        ps.setString(1,"涨\\\\是'");
//        ps.setString(2,null);
//        ps.setString(3,"XZXT.LASDS");
//        ps.setString(4,"1");
//        int result1 = ps.executeUpdate();
//        connection.commit();
//        ps.setString(1,"my_index_dyna222xx");
//        ps.setString(2,"1");
//        ps.setString(3,"1");
//        ps.setString(4,null);
//        int result1 = ps.executeUpdate();
        System.out.println("result = " + result);
//        connection.rollback();
//        Statement statement = connection.createStatement();
//        int result1 = statement.executeUpdate(insertSql6);
//        System.out.println("result1 = " + result1);
//        connection.commit();
        ps.close();
        connection.close();
    }

    @org.junit.Test
    public void testInsertPrepareStatement() throws Exception {
        Class.forName("org.elasticsearch.jdbc.ElasticSearchDriver");
        Connection connection = DriverManager.getConnection("jdbc:elasticsearch://127.0.0.1:9300?" + param);
//        Connection connection = DriverManager.getConnection("jdbc:elasticsearch://localhost:9300","elastic","changeme");
        connection.setAutoCommit(false);
        PreparedStatement ps = connection.prepareStatement("INSERT INTO t_dsmanager_data (DATACODE, DATAROLELEVEL, ID, DATAISRELATION) VALUES ( ?,  ?,  ?,  ?)");
        ps.setString(1, "涨\\\\是'");
        ps.setString(2, null);
        ps.setString(3, "XZXT.LASDS");
        ps.setString(4, "1");
        int result = ps.executeUpdate();
        ps.setString(1,"my_index_dyna222xx");
        ps.setString(2,"1");
        ps.setString(3,"1");
        ps.setString(4,null);
        int result1 = ps.executeUpdate();
        System.out.println("result = " + result);
//        connection.rollback();
//        Statement statement = connection.createStatement();
//        int result1 = statement.executeUpdate(insertSql6);
//        System.out.println("result1 = " + result1);
        connection.commit();
        ps.close();
        connection.close();
    }

    @org.junit.Test
    public void testInsertStatement() throws Exception {
        Class.forName("org.elasticsearch.jdbc.ElasticSearchDriver");
        Connection connection = DriverManager.getConnection("jdbc:elasticsearch://127.0.0.1:9300?" + param);
//        Connection connection = DriverManager.getConnection("jdbc:elasticsearch://localhost:9300","elastic","changeme");
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();
        int result = statement.executeUpdate(insertSql6);
        System.out.println("result = " + result);
        connection.commit();
        statement.close();
        connection.close();
    }

    @org.junit.Test
    public void testGroup() throws Exception {
        System.out.println("group");
        testGroupStatement();
        System.out.println("group1");
        testGroupStatement1();
        System.out.println("group2");
        testGroupStatement2();
    }

    @org.junit.Test
    public void testGroupStatement() throws Exception {
        Class.forName("org.elasticsearch.jdbc.ElasticSearchDriver");
        Connection connection = DriverManager.getConnection("jdbc:elasticsearch://127.0.0.1:9300?" + param);
//        Connection connection = DriverManager.getConnection("jdbc:elasticsearch://localhost:9300","elastic","changeme");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * from  my_index group by key2");
        while (resultSet.next()) {
            ElasticSearchArray elasticSearchArray = (ElasticSearchArray) resultSet.getObject("key2BUCKS");
            List<Map<String, Object>> resultSet1 = (List<Map<String, Object>>) elasticSearchArray.getArray();
            for (Map<String, Object> map : resultSet1) {
                System.out.println("key2KEY:" + map.get("key2KEY"));
                System.out.println("key2COUNT:" + map.get("key2COUNT"));
            }
        }
//        ResultSet resultSet2 = statement.executeQuery(group2);
//        while (resultSet2.next()) {
//            Long array = resultSet2.getLong("stats(key2).count");
//            System.out.println("array = " + array);
//        }
//        ResultSet resultSet3 = statement.executeQuery(group3);
//        while (resultSet3.next()) {
//            Array array = resultSet3.getArray("key2BUCKS");
//            System.out.println("array = " + array);
//        }
        statement.close();
        connection.close();
    }

    @org.junit.Test
    public void testGroupStatement1() throws Exception {
        Class.forName("org.elasticsearch.jdbc.ElasticSearchDriver");
        Connection connection = DriverManager.getConnection("jdbc:elasticsearch://127.0.0.1:9300?" + param);
//        Connection connection = DriverManager.getConnection("jdbc:elasticsearch://localhost:9300","elastic","changeme");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * from  my_index group by key2,key3");
        while (resultSet.next()) {
            resultSet.getFetchSize();
            ElasticSearchArray elasticSearchArray = (ElasticSearchArray) resultSet.getObject("key2BUCKS");
            List<Map<String, Object>> resultSet1 = (List<Map<String, Object>>) elasticSearchArray.getArray();
            for (Map<String, Object> map : resultSet1) {
                System.out.println("key2KEY:" + map.get("key2KEY"));
                System.out.println("key2COUNT:" + map.get("key2COUNT"));
                List<Map<String, Object>> resultSet2 = (List<Map<String, Object>>) map.get("key3BUCKS");
                for (Map<String, Object> map2 : resultSet2) {
                    System.out.println("    key3KEY:" + map2.get("key3KEY"));
                    System.out.println("    key3COUNT:" + map2.get("key3COUNT"));
                }
            }
        }
//        ResultSet resultSet2 = statement.executeQuery(group2);
//        while (resultSet2.next()) {
//            Long array = resultSet2.getLong("stats(key2).count");
//            System.out.println("array = " + array);
//        }
//        ResultSet resultSet3 = statement.executeQuery(group3);
//        while (resultSet3.next()) {
//            Array array = resultSet3.getArray("key2BUCKS");
//            System.out.println("array = " + array);
//        }
        statement.close();
        connection.close();
    }

    @org.junit.Test
    public void testGroupStatement2() throws Exception {
        Class.forName("org.elasticsearch.jdbc.ElasticSearchDriver");
        Connection connection = DriverManager.getConnection("jdbc:elasticsearch://127.0.0.1:9300?" + param);
//        Connection connection = DriverManager.getConnection("jdbc:elasticsearch://localhost:9300","elastic","changeme");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * from  my_index group by (key2),(key3)");
        while (resultSet.next()) {
            ElasticSearchArray elasticSearchArray = (ElasticSearchArray) resultSet.getObject("key2BUCKS");
            List<Map<String, Object>> resultSet1 = (List<Map<String, Object>>) elasticSearchArray.getArray();
            for (Map<String, Object> map : resultSet1) {
                System.out.println("key2KEY:" + map.get("key2KEY"));
                System.out.println("key2COUNT:" + map.get("key2COUNT"));
            }
            ElasticSearchArray elasticSearchArray1 = (ElasticSearchArray) resultSet.getObject("key3BUCKS");
            List<Map<String, Object>> resultSet2 = (List<Map<String, Object>>) elasticSearchArray1.getArray();
            for (Map<String, Object> map2 : resultSet2) {
                System.out.println("key3KEY:" + map2.get("key3KEY"));
                System.out.println("key3COUNT:" + map2.get("key3COUNT"));
            }
        }
//        ResultSet resultSet2 = statement.executeQuery(group2);
//        while (resultSet2.next()) {
//            Long array = resultSet2.getLong("stats(key2).count");
//            System.out.println("array = " + array);
//        }
//        ResultSet resultSet3 = statement.executeQuery(group3);
//        while (resultSet3.next()) {
//            Array array = resultSet3.getArray("key2BUCKS");
//            System.out.println("array = " + array);
//        }
        statement.close();
        connection.close();
    }

    @org.junit.Test
    public void testQueryStatement() throws Exception {
        Class.forName("org.elasticsearch.jdbc.ElasticSearchDriver");
        Connection connection = DriverManager.getConnection("jdbc:elasticsearch://127.0.0.1:9300?" + param);
//        Connection connection = DriverManager.getConnection("jdbc:elasticsearch://localhost:9300","elastic","changeme");
        Statement statement = connection.createStatement();
        ResultSet resultSet1 = statement.executeQuery(query1);
        while (resultSet1.next()) {
            String key1 = resultSet1.getString("key1");
        }
//        ResultSet resultSet2 = statement.executeQuery(group2);
//        while (resultSet2.next()) {
//            Long array = resultSet2.getLong("stats(key2).count");
//            System.out.println("array = " + array);
//        }
//        ResultSet resultSet3 = statement.executeQuery(group3);
//        while (resultSet3.next()) {
//            Array array = resultSet3.getArray("key2BUCKS");
//            System.out.println("array = " + array);
//        }
        statement.close();
        connection.close();
    }

    @org.junit.Test
    public void testQueryPrepareStatement() throws Exception {
        Class.forName("org.elasticsearch.jdbc.ElasticSearchDriver");
        Connection connection = DriverManager.getConnection("jdbc:elasticsearch://127.0.0.1:9300?" + param);
        PreparedStatement ps = connection.prepareStatement(query2);
        ResultSet resultSet = ps.executeQuery();
        List<String> result = new ArrayList<String>();
        while (resultSet.next()) {
            System.out.println(resultSet.getObject("key2"));
            System.out.println(resultSet.getString("key1"));
        }
        ps.close();
        connection.close();
    }
}
