package org.elasticsearch.jdbc;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zy-xx on 2019/10/10.
 */
public class ElasticSearchArray implements Array {

    private int baseType = 0;

    private String baseTypeName = "";

    private ElasticSearchResultSet elasticSearchResultSet;

    private List<String> headers = new ArrayList<>();
    private List<List<Object>> lines = new ArrayList<>();

    public ElasticSearchArray(List<String> headers, List<List<Object>> lines) {
        if (headers != null) {
            this.headers = headers;
        }
        if (lines != null) {
            this.lines = lines;
        }
        elasticSearchResultSet = new ElasticSearchResultSet(this.headers, this.lines);
    }

    public ElasticSearchArray() {
        elasticSearchResultSet = new ElasticSearchResultSet(this.headers, this.lines);
    }

    public void addHeaders(String columnName) {
        headers.add(columnName);
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        return baseTypeName;
    }

    @Override
    public int getBaseType() throws SQLException {
        return baseType;
    }

    @Override
    public Object getArray() throws SQLException {
        List<Map<String, Object>> result = new ArrayList<>();
        for (List<Object> lists : lines) {
            Map<String, Object> resultMap = new HashMap<>();
            for (int i = 0; i < headers.size(); i++) {
                String key = headers.get(i);
                Object value = lists.get(i);
                resultMap.put(key, value);
            }
            result.add(resultMap);
        }
        return result;
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return elasticSearchResultSet;
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void free() throws SQLException {
        headers.clear();
        lines.clear();
        elasticSearchResultSet.close();
    }

    public List<String> getHeaders() {
        return headers;
    }

    public List<List<Object>> getLines() {
        return lines;
    }

    @Override
    public String toString() {
        return "ElasticSearchArray{" +
                "headers=" + headers +
                ", lines=" + lines +
                '}';
    }
}
