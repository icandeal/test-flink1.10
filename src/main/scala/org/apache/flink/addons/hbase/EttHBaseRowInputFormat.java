package org.apache.flink.addons.hbase;

import org.apache.flink.addons.hbase.util.HBaseReadWriteHelper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

import static org.apache.flink.table.descriptors.EttHBaseValidator.CONNECTOR_ZK_NODE_PARENT;
import static org.apache.flink.table.descriptors.EttHBaseValidator.CONNECTOR_ZK_QUORUM;

/**
 * Created by yuchunfan on 2020/4/30.
 */
public class EttHBaseRowInputFormat extends AbstractTableInputFormat<Row> implements ResultTypeQueryable<Row> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(HBaseRowInputFormat.class);

    private final String tableName;
    private final HBaseTableSchema schema;

    private Properties prop;
    private transient HBaseReadWriteHelper readHelper;

    public EttHBaseRowInputFormat(Properties prop, String tableName, HBaseTableSchema schema) {
        this.tableName = tableName;
        this.prop = prop;
        this.schema = schema;
    }

    @Override
    public void configure(Configuration parameters) {
        LOG.info("Initializing HBase configuration.");
        // prepare hbase read helper
        this.readHelper = new HBaseReadWriteHelper(schema);
        connectToTable();
        if (table != null) {
            scan = getScanner();
        }
    }

    @Override
    protected Scan getScanner() {
        return readHelper.createScan();
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    protected Row mapResultToOutType(Result res) {
        return readHelper.parseToRow(res);
    }

    private void connectToTable() {
        try {
            org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
            String hbaseZk = this.prop.getProperty(CONNECTOR_ZK_QUORUM);
            conf.set(HConstants.ZOOKEEPER_QUORUM, hbaseZk);
            String zkNodeParent = this.prop.getProperty(CONNECTOR_ZK_NODE_PARENT);
            if (zkNodeParent != null)conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, zkNodeParent);
            Connection conn = ConnectionFactory.createConnection(conf);
            super.table = (HTable) conn.getTable(TableName.valueOf(tableName));
        } catch (TableNotFoundException tnfe) {
            LOG.error("The table " + tableName + " not found ", tnfe);
            throw new RuntimeException("HBase table '" + tableName + "' not found.", tnfe);
        } catch (IOException ioe) {
            LOG.error("Exception while creating connection to HBase.", ioe);
            throw new RuntimeException("Cannot create connection to HBase.", ioe);
        }
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        // split the fieldNames
        String[] famNames = schema.getFamilyNames();
        TypeInformation<?>[] typeInfos = new TypeInformation[famNames.length];
        int i = 0;
        for (String family : famNames) {
            typeInfos[i] = new RowTypeInfo(schema.getQualifierTypes(family), schema.getQualifierNames(family));
            i++;
        }
        return new RowTypeInfo(typeInfos, famNames);
    }
}
