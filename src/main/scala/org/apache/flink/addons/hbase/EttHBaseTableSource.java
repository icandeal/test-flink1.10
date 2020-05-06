package org.apache.flink.addons.hbase;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

import java.util.Arrays;
import java.util.Properties;

import static org.apache.flink.table.descriptors.EttHBaseValidator.CONNECTOR_ZK_NODE_PARENT;
import static org.apache.flink.table.descriptors.EttHBaseValidator.CONNECTOR_ZK_QUORUM;

/**
 * Created by yuchunfan on 2020/4/30.
 */
public class EttHBaseTableSource implements BatchTableSource<Row>, ProjectableTableSource<Row>, StreamTableSource<Row>, LookupableTableSource<Row> {

    private final Configuration conf;
    private final Properties prop;
    private final String tableName;
    private final HBaseTableSchema hbaseSchema;
    private final int[] projectFields;

    /**
     * The HBase configuration and the name of the table to read.
     *
     * @param prop      hbase configuration
     * @param tableName the tableName
     */
    public EttHBaseTableSource(Properties prop, String tableName) {
        this(prop, tableName, new HBaseTableSchema(), null);
    }

    public EttHBaseTableSource(Properties prop, String tableName, HBaseTableSchema hbaseSchema, int[] projectFields) {
        this.prop = prop;
        this.conf = HBaseConfiguration.create();
        String hbaseZk = this.prop.getProperty(CONNECTOR_ZK_QUORUM);
        conf.set(HConstants.ZOOKEEPER_QUORUM, hbaseZk);
        String zkNodeParent = this.prop.getProperty(CONNECTOR_ZK_NODE_PARENT);
        if (zkNodeParent != null)conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, zkNodeParent);
        this.tableName = Preconditions.checkNotNull(tableName, "Table  name");
        this.hbaseSchema = hbaseSchema;
        this.projectFields = projectFields;
    }

    /**
     * Adds a column defined by family, qualifier, and type to the table schema.
     *
     * @param family    the family name
     * @param qualifier the qualifier name
     * @param clazz     the data type of the qualifier
     */
    public void addColumn(String family, String qualifier, Class<?> clazz) {
        this.hbaseSchema.addColumn(family, qualifier, clazz);
    }

    /**
     * Sets row key information in the table schema.
     * @param rowKeyName the row key field name
     * @param clazz the data type of the row key
     */
    public void setRowKey(String rowKeyName, Class<?> clazz) {
        this.hbaseSchema.setRowKey(rowKeyName, clazz);
    }

    /**
     * Specifies the charset to parse Strings to HBase byte[] keys and String values.
     *
     * @param charset Name of the charset to use.
     */
    public void setCharset(String charset) {
        this.hbaseSchema.setCharset(charset);
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        HBaseTableSchema projectedSchema = hbaseSchema.getProjectedHBaseTableSchema(projectFields);
        return projectedSchema.convertsToTableSchema().toRowType();
    }

    @Override
    public TableSchema getTableSchema() {
        return hbaseSchema.convertsToTableSchema();
    }

    @Override
    public DataSet<Row> getDataSet(ExecutionEnvironment execEnv) {
        HBaseTableSchema projectedSchema = hbaseSchema.getProjectedHBaseTableSchema(projectFields);
        return execEnv
            .createInput(new EttHBaseRowInputFormat(prop, tableName, projectedSchema), getReturnType())
            .name(explainSource());
    }

    @Override
    public org.apache.flink.addons.hbase.EttHBaseTableSource projectFields(int[] fields) {
        return new org.apache.flink.addons.hbase.EttHBaseTableSource(this.prop, tableName, hbaseSchema, fields);
    }

    @Override
    public String explainSource() {
        return "EttHBaseTableSource[schema=" + Arrays.toString(getTableSchema().getFieldNames())
            + ", projectFields=" + Arrays.toString(projectFields) + "]";
    }

    @Override
    public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
        Preconditions.checkArgument(
            null != lookupKeys && lookupKeys.length == 1,
            "HBase table can only be retrieved by rowKey for now.");
        Preconditions.checkState(
            hbaseSchema.getRowKeyName().isPresent(),
            "HBase schema must have a row key when used in lookup mode.");
        Preconditions.checkState(
            hbaseSchema.getRowKeyName().get().equals(lookupKeys[0]),
            "The lookup key is not row key of HBase.");

        return new HBaseLookupFunction(
            this.conf,
            this.tableName,
            hbaseSchema.getProjectedHBaseTableSchema(projectFields));
    }

    @Override
    public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
        throw new UnsupportedOperationException("HBase table doesn't support async lookup currently.");
    }

    @Override
    public boolean isAsyncEnabled() {
        return false;
    }

    @Override
    public boolean isBounded() {
        // HBase source is always bounded.
        return true;
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        HBaseTableSchema projectedSchema = hbaseSchema.getProjectedHBaseTableSchema(projectFields);
        return execEnv
            .createInput(new EttHBaseRowInputFormat(this.prop, tableName, projectedSchema), getReturnType())
            .name(explainSource());
    }

    @VisibleForTesting
    HBaseTableSchema getHBaseTableSchema() {
        return this.hbaseSchema;
    }
}
