package org.nlpcn.es4sql.query;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;

/**
 * Created by Eliran on 19/8/2015.
 */
public class SqlElasticUpdateByQueryRequestBuilder implements SqlElasticRequestBuilder {
    UpdateByQueryRequest updateByQueryRequestBuilder;

    public SqlElasticUpdateByQueryRequestBuilder(UpdateByQueryRequest updateByQueryRequestBuilder) {
        this.updateByQueryRequestBuilder = updateByQueryRequestBuilder;
    }

    @Override
    public ActionRequest request() {
        return updateByQueryRequestBuilder;
    }

    @Override
    public String explain() {
        return null;
    }

    @Override
    public ActionResponse get() {

        return null;
    }

    @Override
    public ActionRequestBuilder getBuilder() {
        return null;
    }

}
