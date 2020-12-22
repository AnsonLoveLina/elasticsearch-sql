package org.nlpcn.es4sql;

import com.google.common.collect.Lists;
import com.ngw.SqlUtil;
import org.elasticsearch.client.RestHighLevelClient;

import java.net.URI;

public class TestSqlUtil {

    @org.junit.Test
    public void testRequestSql() throws Exception {
        String result = SqlUtil.requestSql("select * from my_index1", new RestHighLevelClient(SqlUtil.getRestClientBuilder("elastic","xx198742",Lists.newArrayList(URI.create("http://localhost:9200")))));
        System.out.println("result = " + result);
    }
    @org.junit.Test
    public void testRequestSqlGroupby() throws Exception {
        String result = SqlUtil.requestSql("SELECT * from  my_index group by key2,key3", new RestHighLevelClient(SqlUtil.getRestClientBuilder("elastic","xx198742",Lists.newArrayList(URI.create("http://localhost:9200")))));
        System.out.println("result = " + result);
    }
}
