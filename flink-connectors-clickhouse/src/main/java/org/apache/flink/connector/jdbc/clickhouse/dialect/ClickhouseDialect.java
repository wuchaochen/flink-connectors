package org.apache.flink.connector.jdbc.clickhouse.dialect;

import org.apache.flink.connector.jdbc.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.dialect.AbstractDialect;
import org.apache.flink.connector.jdbc.clickhouse.ClickhouseConvertor;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

public class ClickhouseDialect extends AbstractDialect {
    @Override
    public Set<org.apache.flink.table.types.logical.LogicalTypeRoot> supportedTypes() {
        return EnumSet.of(
                LogicalTypeRoot.CHAR,
                LogicalTypeRoot.VARCHAR,
                LogicalTypeRoot.BOOLEAN,
                LogicalTypeRoot.VARBINARY,
                LogicalTypeRoot.DECIMAL,
                LogicalTypeRoot.TINYINT,
                LogicalTypeRoot.SMALLINT,
                LogicalTypeRoot.INTEGER,
                LogicalTypeRoot.BIGINT,
                LogicalTypeRoot.FLOAT,
                LogicalTypeRoot.DOUBLE,
                LogicalTypeRoot.DATE,
                LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
                LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
        );
    }

    @Override
    public String dialectName() {
        return "ClickHouse";
    }

    @Override
    public JdbcRowConverter getRowConverter(org.apache.flink.table.types.logical.RowType rowType) {
        return new ClickhouseConvertor(rowType);
    }

    @Override
    public String getLimitClause(long limit) {
        return "LIMIT " + limit;
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.clickhouse.jdbc.ClickHouseDriver");
    }

    @Override
    public String quoteIdentifier(String s) {
        return s;
    }

    @Override
    public Optional<String> getUpsertStatement(String s, String[] strings, String[] strings1) {
        throw new UnsupportedOperationException("Clickhouse does not support update sql");
    }
}
