package org.apache.flink.connector.jdbc.clickhouse;

import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.types.logical.RowType;

public class ClickhouseConvertor extends AbstractJdbcRowConverter {
    public ClickhouseConvertor(RowType rowType) {
        super(rowType);
    }

    @Override
    public String converterName() {
        return "ClickHouse";
    }
}
