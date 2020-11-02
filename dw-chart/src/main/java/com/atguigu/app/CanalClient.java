//package com.atguigu.app;
//
//import com.alibaba.fastjson.JSONObject;
//import com.alibaba.otter.canal.client.CanalConnector;
//import com.alibaba.otter.canal.client.CanalConnectors;
//import com.alibaba.otter.canal.protocol.CanalEntry;
//import com.alibaba.otter.canal.protocol.Message;
//import com.atguigu.constants.GmallConstants;
//import com.atguigu.utils.KafkaSender;
//import com.google.protobuf.InvalidProtocolBufferException;
//
//import java.net.InetSocketAddress;
//import java.util.List;
//
//public class CanalClient {
//    public static void main(String[] args) {
//
//        //获取Canal连接
//        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");
//
//        //抓取数据
//        while (true) {
//
//            //连接
//            canalConnector.connect();
//            Message message = canalConnector.get(100);
//
//            //订阅表
//            canalConnector.subscribe("gmall.*");
//
//            //判断是否有数据
//            if (message.getEntries().size() == 0) {
//                System.out.println("没有数据，休息一下");
//            } else {
//
//                //TODO 数据处理
//                //遍历message中的entry
//                for (CanalEntry.Entry entry : message.getEntries()) {
//
//                    //过滤掉写操作中操作数据的操作，如：事务的开启关，闭
//                    if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) {
//                        CanalEntry.RowChange rowChange = null;
//                        try {
//
//                            //反序列化
//                            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
//
//                            //insert,delete,update
//                            CanalEntry.EventType eventType = rowChange.getEventType();
//
//                            String tableName = entry.getHeader().getTableName();
//                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
//
//                            //解析数据
//                            hadler(tableName, eventType, rowDatasList);
//
//                        } catch (InvalidProtocolBufferException e) {
//                            e.printStackTrace();
//                        }
//
//                    }
//
//                }
//            }
//        }
//
//    }
//
//
//    //GMV交易额
//    private static void hadler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
//
//        if ("order_info".equals(tableName)) {
//
//            //过滤出下单数据，及类型为insert
//            if (eventType.equals(CanalEntry.EventType.INSERT)) {
//
//                //遍历rowDatalist
//                for (CanalEntry.RowData rowData : rowDatasList) {
//                    List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
//
//
//                    JSONObject jsonObject = new JSONObject();
//                    for (CanalEntry.Column column : afterColumnsList) {
//                        jsonObject.put(column.getName(), column.getValue());
//                    }
//
//                    // jsonObject.put("tableName", tableName);
//
//                    KafkaSender.send(GmallConstants.GMALL_ORDER_INFO,jsonObject.toJSONString());
//                    //打印查看
//                    System.out.println(jsonObject.toJSONString());
//
//                }
//            }
//        }
//    }
//}
//
