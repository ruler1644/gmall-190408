package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.KafkaSender;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {
    public static void main(String[] args) {

        //获取Canal连接
        InetSocketAddress ineAddress = new InetSocketAddress("hadoop102", 11111);
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(ineAddress, "example", "", "");

        //抓取数据
        while (true) {

            //连接
            canalConnector.connect();

            //订阅表，抓取数据，一次100条
            canalConnector.subscribe("gmall.*");
            Message message = canalConnector.get(100);

            //判断是否有数据
            if (message.getEntries().size() == 0) {
                System.out.println("没有数据，休息一下");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            } else {

                //TODO 数据处理
                //遍历message中的entry
                for (CanalEntry.Entry entry : message.getEntries()) {

                    //过滤掉写操作中，不是操作数据的操作，如：事务的开启、关闭、心跳、锁
                    if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) {
                        CanalEntry.RowChange rowChange = null;
                        try {

                            //反序列化
                            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());

                            //insert、delete、update
                            CanalEntry.EventType eventType = rowChange.getEventType();
                            String tableName = entry.getHeader().getTableName();
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                            //解析数据
                            handler(tableName, eventType, rowDatasList);

                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }


    //解析数据，将数据发送到Kafka
    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {

        //订单表，取下单数据
        if ("order_info".equals(tableName) && eventType.equals(CanalEntry.EventType.INSERT)) {
            sendToKafka(rowDatasList, GmallConstants.GMALL_ORDER_INFO);
        }

        //订单详情表，取新增数据
        else if ("order_detail".equals(tableName) && eventType.equals(CanalEntry.EventType.INSERT)) {
            sendToKafka(rowDatasList, GmallConstants.GMALL_ORDER_DETAIL);
        }

        //用户信息表，取新增及变化数据
        else if ("user_info".equals(tableName) && (eventType.equals(CanalEntry.EventType.INSERT) || eventType.equals(CanalEntry.EventType.UPDATE))) {
            sendToKafka(rowDatasList, GmallConstants.GMALL_USER_INFO);
        }
    }

    //将数据发送到Kafka
    private static void sendToKafka(List<CanalEntry.RowData> rowDatasList, String topic) {

        //遍历rowDataList
        for (CanalEntry.RowData rowData : rowDatasList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

            //JSon
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                jsonObject.put(column.getName(), column.getValue());
            }

            //测试
            System.out.println("打印数据测试：" + jsonObject.toJSONString());
            KafkaSender.send(topic, jsonObject.toJSONString());
        }
    }
}

