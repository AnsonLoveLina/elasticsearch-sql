package org.nlpcn.es4sql.index;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;

/**
 * Created by zy-xx on 2019/9/26.
 */
public class SqlElasticInsertRequestBuilder implements SqlElasticRequestBuilder {
    private IndexRequest indexRequestBuilder;

    public SqlElasticInsertRequestBuilder(IndexRequest indexRequestBuilder) {
        this.indexRequestBuilder = indexRequestBuilder;
    }

    @Override
    public ActionRequest request() {
        return indexRequestBuilder;
    }

    @Override
    public String explain() {
//        try {
//            if (request() != null) {
//                return indexRequestBuilder.request().source().toString();
//            }
//            return indexRequestBuilder.toString();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        return null;
    }

    @Override
    public ActionResponse get() {
//        return indexRequestBuilder.get();
        return null;
    }

    @Override
    public ActionRequestBuilder getBuilder() {
//        return indexRequestBuilder;
        return null;
    }

    @Override
    public String toString() {
//        try {
//            if (request() != null) {
//                return indexRequestBuilder.request().source().toString();
//            }
//            return indexRequestBuilder.toString();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        return null;
    }
}
