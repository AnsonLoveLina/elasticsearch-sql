package com.ngw;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

/**
 * Created by zy-xx on 2020/8/27.
 */
public class BulkProcessorProxy {
    public static BulkProcessor getBulkprocessor(Client client) {

        return BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
//                System.out.println("request");
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
//                System.out.println("response");

            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
//                System.out.println("failure" + failure);

            }
        }).setBulkActions(5000).setFlushInterval(TimeValue.timeValueSeconds(60)).setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB)).setConcurrentRequests(3).setBackoffPolicy(
                BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)).build();
    }
}
