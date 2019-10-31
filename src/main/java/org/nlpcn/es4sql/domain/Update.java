package org.nlpcn.es4sql.domain;

import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;

import java.util.List;

/**
 * SQL Delete statement.
 */
public class Update extends Query {
    private List<SQLUpdateSetItem> items;

    public List<SQLUpdateSetItem> getItems() {
        return items;
    }

    public void setItems(List<SQLUpdateSetItem> items) {
        this.items = items;
    }
}
