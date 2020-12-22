package com.ngw;

import jodd.util.StringUtil;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.*;
import org.nlpcn.es4sql.Action;
import org.nlpcn.es4sql.SearchDao;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.jdbc.ObjectResultsExtractor;
import org.nlpcn.es4sql.query.AggregationQueryAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.Map;

public class SqlUtil {
    private static Logger logger = LoggerFactory.getLogger(SqlUtil.class);

    public static RestClient getRestClient(List<URI> uriList) {
        return getRestClientBuilder(null, null, uriList).build();
    }

    public static RestClient getRestClient(String user, String password, List<URI> uriList) {
        return getRestClientBuilder(user, password, uriList).build();
    }

    public static RestClientBuilder getRestClientBuilder(List<URI> uriList) {
        return getRestClientBuilder(null, null, uriList);
    }

    public static RestClientBuilder getRestClientBuilder(String user, String password, List<URI> uriList) {
        CredentialsProvider credentialsProvider = null;
        if (StringUtil.isNotBlank(user)) {
            credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(user, password));
        }

        HttpHost[] httpHosts = new HttpHost[uriList.size()];
        for (int i = 0; i < uriList.size(); ++i) {
            httpHosts[i] = new HttpHost(uriList.get(i).getHost(), uriList.get(i).getPort(), "http");
        }

        CredentialsProvider finalCredentialsProvider = credentialsProvider;
        return RestClient.builder(httpHosts)
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        if (finalCredentialsProvider != null) {
                            return httpClientBuilder.setDefaultCredentialsProvider(finalCredentialsProvider);
                        } else {
                            return httpClientBuilder;
                        }
                    }
                }).setFailureListener(new RestClient.FailureListener() {

                    @Override
                    public void onFailure(Node node) {
                        logger.error("node left:", node);
                    }
                });
    }

    public static String requestSql(String sql, RestHighLevelClient restHighLevelClient) {
        SearchDao searchDao = new SearchDao(restHighLevelClient);

        try {
            Action action = searchDao.explain(sql);
            SearchResponse searchResponse = restHighLevelClient.search((SearchRequest) action.explain().request(), RequestOptions.DEFAULT);
            return searchResponse.toString();
        } catch (SqlParseException | SQLFeatureNotSupportedException | IOException throwables) {
            throwables.printStackTrace();
        }
        return null;
    }

    public static String request(String url, Map<String, String> headers, String body, Constant.Method method, RestClient restClient) {
        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        if (headers != null) {
            for (Map.Entry<String, String> header : headers.entrySet()) {
                options.addHeader(header.getKey(), header.getValue());
            }
        }

        HttpEntity entity = new NStringEntity(body, ContentType.APPLICATION_JSON);
        Request request = new Request(method.getString(), url);
        request.setEntity(entity);
        request.setOptions(options);
        try {
            Response response = restClient.performRequest(request);
            return EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
