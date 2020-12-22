package org.nlpcn.es4sql.index;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.nlpcn.es4sql.domain.Insert;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.Action;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;

/**
 * Created by zy-xx on 2019/9/26.
 * update by query和delete by query不算在index中，所以只有insert对于ES而言是insertOrUpdate
 */
public class InsertAction implements IndexAction, Action {
    private Insert insert;
    private IndexRequest request;

    public InsertAction(Insert insert) {
        this.insert = insert;
    }

    @Override
    public SqlElasticRequestBuilder explain() throws SqlParseException {

        this.request = new IndexRequest(insert.getIndex(), insert.getType(), insert.getId());

        setValues();

        SqlElasticInsertRequestBuilder sqlElasticInsertRequestBuilder = new SqlElasticInsertRequestBuilder(request);
        return sqlElasticInsertRequestBuilder;
    }

    public Insert getInsert() {
        return insert;
    }

    private void setValues() {
        this.request.source(insert.getValues());
    }

    @Override
    public int getCount() {
        return insert.getValues() == null ? 0 : 1;
    }
}
