package com.rayfay.nrap.flink.snappydata;

import org.apache.flink.types.Row;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.calcite.shaded.com.google.common.collect.Queues;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple3;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class SnappyDataInputFormat implements InputFormat<Row, SnappyTableInputSplit> {
    //logger
    private static final Logger LOGGER = LoggerFactory.getLogger(SnappyDataInputFormat.class);

    public static final String jdbcUrlKey = "SN_JDBC_URL";
    public static final String jdbcTableKey = "SN_TABLE";
    public static final String jdbcTableSql = "SN_SQL";
    private SnappydataJdbcUtil snappydataJdbc;
    private String connectionURL;
    private String tableName;
    private String sqlText;
    private JDBCOptions jdbcOptions;
    private LinkedBlockingQueue<Row> rowBuffer = Queues.newLinkedBlockingQueue();

    //data
    private boolean end = false;


    @Override
    public void configure(Configuration configuration) {
        connectionURL = configuration.getString(jdbcUrlKey, "");
        assert StringUtils.isNoneBlank(connectionURL);

        tableName = configuration.getString(jdbcTableKey, "");
        sqlText = configuration.getString(jdbcTableSql, "");

        jdbcOptions = new JDBCOptions(connectionURL, Maps.newHashMap());
        snappydataJdbc = new SnappydataJdbcUtil(jdbcOptions);
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return null;
    }

    @Override
    public SnappyTableInputSplit[] createInputSplits(int numSplits) throws IOException {
       System.out.println("required createInputSplits.numSplits="+numSplits);
       SnappyTableInputSplit[] inputSplits = snappydataJdbc.partitions(tableName);
       System.out.println("createInputSplits.inputSplits.length ="+inputSplits.length);
       return inputSplits;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(SnappyTableInputSplit[] snappyTableInputSplits) {
        return new SnappydataInputSplitAssigner(snappyTableInputSplits);
    }

    @Override
    public void open(SnappyTableInputSplit split) throws IOException {
        LOGGER.info("reading split index={}", split.index());
        SnappyDataConnectorHelper helper = new SnappyDataConnectorHelper();
        Properties  properties = jdbcOptions.asConnectionProperties();
        Map<String, String> poolMaps = Maps.newHashMap();
        ConnectionProperties connectionProperties = new ConnectionProperties(connectionURL, jdbcOptions.getJdbcDriverClass()
                , (poolMaps), properties, properties, true);

        Connection connection = helper.getConnection(connectionProperties, split);

        String txId = helper.refreshNewTransactionId();
        LOGGER.info("SnappydataInputFormat Using txId={}", txId);

        if(StringUtils.isBlank(sqlText)){
            sqlText = "select * from "+tableName;
        }
        LOGGER.info("reading split index={}, sqlText={}", split.index(), sqlText);
        Tuple3<Statement, ResultSet, String> queryResult = helper.executeQuery(connection, tableName, split, sqlText, -1);
        ResultSet rs = queryResult._2();
        int rowsCount = 0;
        try {
            int columnCount = rs.getMetaData().getColumnCount();
            while(rs.next()){
                rowsCount ++ ;
                Object[] rows = new Object[columnCount];
                for (int i = 1; i <= columnCount; i++) {
                    rows[i-1] = rs.getObject(i);
                }
                rowBuffer.offer(Row.of(rows));
            }
            this.end = true;
            LOGGER.info("found {} records in split {}", rowsCount, split.getSplitNumber());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //
        if(helper !=null){
            helper.commitBeforeCloseConnection(connection);
            helper.closeConnection(connection);
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return rowBuffer.isEmpty() && end;
    }

    @Override
    public Row nextRecord(Row row) throws IOException {
        try {
            return rowBuffer.poll(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        if(snappydataJdbc != null){
            snappydataJdbc.commitAndClose();
        }
    }
}
