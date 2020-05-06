package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

/**
 * Created by yuchunfan on 2020/4/30.
 */
@Internal
public class EttHBaseValidator extends ConnectorDescriptorValidator {
    public static final String CONNECTOR_TYPE_VALUE_HBASE = "etthbase";
    public static final String CONNECTOR_TABLE_NAME = "connector.table-name";
    public static final String CONNECTOR_ZK_QUORUM = "connector.zookeeper.quorum";
    public static final String CONNECTOR_ZK_NODE_PARENT = "connector.zookeeper.znode.parent";
    public static final String CONNECTOR_WRITE_BUFFER_FLUSH_MAX_SIZE = "connector.write.buffer-flush.max-size";
    public static final String CONNECTOR_WRITE_BUFFER_FLUSH_MAX_ROWS = "connector.write.buffer-flush.max-rows";
    public static final String CONNECTOR_WRITE_BUFFER_FLUSH_INTERVAL = "connector.write.buffer-flush.interval";

    public EttHBaseValidator() {
    }

    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        properties.validateValue("connector.type", CONNECTOR_TYPE_VALUE_HBASE, false);
        properties.validateString("connector.table-name", false, 1);
        properties.validateString("connector.zookeeper.quorum", false, 1);
        properties.validateString("connector.zookeeper.znode.parent", true, 1);
        this.validateSinkProperties(properties);
    }

    private void validateSinkProperties(DescriptorProperties properties) {
        properties.validateMemorySize("connector.write.buffer-flush.max-size", true, 1048576);
        properties.validateInt("connector.write.buffer-flush.max-rows", true, 1);
        properties.validateDuration("connector.write.buffer-flush.interval", true, 1);
    }
}
