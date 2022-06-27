package com.four5prings.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.four5prings.constants.GmallConstants;
import com.four5prings.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @ClassName CanalClient
 * @Description
 * @Author Four5prings
 * @Date 2022/6/27 22:24
 * @Version 1.0
 */
public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //1. 获取canal连接对象
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        while (true) {
            //2 获取连接
            canalConnector.connect();

            canalConnector.subscribe("gmall2022.*");

            Message message = canalConnector.get(100);

            List<CanalEntry.Entry> entries = message.getEntries();
            //进行健壮性判断，如果entries中没有数据，我们可以线程等待5s
            if (entries.size() <= 0) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                for (CanalEntry.Entry entry : entries) {
                    //TODO 获取表名
                    String tableName = entry.getHeader().getTableName();

                    CanalEntry.EntryType entryType = entry.getEntryType();
                    if (entryType.equals(CanalEntry.EntryType.ROWDATA)) {
                        //序列化
                        ByteString storeValue = entry.getStoreValue();
                        //反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        //TODO 获取数据类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //TODO 获取rowDataList数据
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        //TODO 根据条件获取数据
                        handler(tableName,eventType,rowDatasList);
                    }
                }
            }

        }
    }

    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        //获取订单表中的新增数据
        if ("order_info".equals(tableName)&&eventType.equals(CanalEntry.EventType.INSERT)){
            for (CanalEntry.RowData rowData : rowDatasList) {
                //获取存放每个列的集合
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                //获取每个列
                JSONObject jsonObject = new JSONObject();
                for (CanalEntry.Column column : afterColumnsList) {
                    jsonObject.put(column.getName(),column.getValue());
                }
                System.out.println(jsonObject.toString());
                MyKafkaSender.send(GmallConstants.KAFKA_TOPIC_ORDER,jsonObject.toString());
            }
        }
    }
}
