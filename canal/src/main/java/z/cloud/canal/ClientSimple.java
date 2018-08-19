package z.cloud.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
//import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.google.gson.Gson;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sun.rowset.internal.Row;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.StreamHandler;


/**
 * Created by cloud on 17/11/8.
 */
public class ClientSimple {
    public static void main(String[] args){
        CanalConnector connector= CanalConnectors.newSingleConnector(new InetSocketAddress(args[0],Integer.parseInt(args[1])),args[2],"","");
        int batchSize=1000;
        int emptyCount=0;
        Gson gson = new Gson();
        try{
            connector.connect();
            connector.subscribe(".*\\..*");
            connector.rollback();
            int totalemtryCount=1200;
            while(emptyCount<totalemtryCount){
                Message message=connector.getWithoutAck(batchSize);
                long batchId=message.getId();
                int size=message.getEntries().size();
                if(batchId==-1 && size==0){
                    emptyCount++;
                    System.out.println("Empty count : " + emptyCount);
                    try{
                        Thread.sleep(1000);
                    }catch (InterruptedException e){
                        e.printStackTrace();
                    }
                }else {
                    emptyCount=0;
                    printEntry(message.getEntries(),gson);
                }
                connector.ack(batchId);
                //connector.rollback(batchId);
            }
        }catch (Exception e){
            System.out.println(e.toString());
        }finally {
            connector.disconnect();
        }
    }

    private static void printEntry(List<Entry> entrys,Gson gson){
        for(Entry entry : entrys){
            if(entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND){
                continue;
            }
            RowChange rowChange = null;
            try{
                rowChange=RowChange.parseFrom(entry.getStoreValue());
            }catch (InvalidProtocolBufferException e){
                System.out.println("entry string : "  + entry.toString() + " error : " + e.toString());
            }

            EventType eventType = rowChange.getEventType();
/*            TableToJson ttj = createToJson(
                    entry.getHeader().getSchemaName(),
                    entry.getHeader().getTableName(),
                    entry.getHeader().getLogfileName(),
                    String.valueOf(entry.getHeader().getLogfileOffset()));
            System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));*/
            Map<String,Object> hm = headerMap(entry,eventType);
            for(RowData rowData : rowChange.getRowDatasList()){
                printJson(eventType,rowData,hm,gson);
            }
        }
    }

    private static Map<String,Object> tableMap(List<Column> cs){
        Map<String,Object> tm = new HashMap<String,Object>();
        for(int i=0;i<cs.size();i++){
            tm.put(cs.get(i).getName(),cs.get(i).getValue());
        }
        return tm;
    }

    private static Map<String,Object> headerMap(Entry entry,EventType eventType){
        Map<String,Object> hm = new HashMap<String,Object>();

        String database = entry.getHeader().getSchemaName();
        String table = entry.getHeader().getTableName();

        hm.put("DATABASE",database);
        hm.put("GLOBAL_ID",database+"-"+table+"-0-"+TableToJson.getGlobalId()+"-0");
        hm.put("EVENT_SERVER_ID","0x002");
        hm.put("BINLOG_NAME",entry.getHeader().getLogfileName());
        hm.put("BINLOG_POS",entry.getHeader().getLogfileOffset());
        hm.put("TIME",myTime());
        hm.put("TABLE",table);
        hm.put("GROUP_ID","0x001");
        if(eventType == EventType.INSERT) {
            hm.put("TYPE","I");
        }else if(eventType == EventType.UPDATE){
            hm.put("TYPE","U");
        }else if(eventType == EventType.DELETE){
            hm.put("TYPE","D");
        }
        return hm;
    }

    private static String myTime(){
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
        return formatter.format(currentTime);
    }

    private static void printColumn(List<Column> cs){
        for(int i=0;i<cs.size();i++){
            Column c = cs.get(i);
            System.out.print(c.getName()+":"+c.getValue()+" "+c.getMysqlType()+" ");
        }
        System.out.println("");
    }

    private static void printJson(EventType eventType,RowData rowData,Map<String,Object> headerMap,Gson gson){
        if(eventType == EventType.INSERT) {
            Map<String, Object> tm = tableMap(rowData.getAfterColumnsList());
            headerMap.put("NEW_VALUES", tm);
            headerMap.remove("OLD_VALUES");
        }else if(eventType == EventType.UPDATE){
            Map<String, Object> tma = tableMap(rowData.getAfterColumnsList());
            Map<String, Object> tmo = tableMap(rowData.getBeforeColumnsList());
            headerMap.put("NEW_VALUES",tma);
            headerMap.put("OLD_VALUES",tmo);
        }else if(eventType == EventType.DELETE){
            Map<String, Object> tm = tableMap(rowData.getBeforeColumnsList());
            headerMap.put("OLD_VALUES",tm);
            headerMap.remove("NEW_VALUES");
        }
        System.out.println(gson.toJson(headerMap));
    }

    private static void printJson(EventType eventType,RowData rowData, TableToJson ttj, Gson gson){
        if(eventType == EventType.INSERT) {
            Table t = createTable(rowData.getAfterColumnsList());
            ttj.setNEW_VALUES(t);
        }else if(eventType == EventType.UPDATE){
            Table ta = createTable(rowData.getAfterColumnsList());
            Table to = createTable(rowData.getBeforeColumnsList());
            ttj.setNEW_VALUES(ta);
            ttj.setOLD_VALUES(to);
        }else if(eventType == EventType.DELETE){
            Table t = createTable(rowData.getBeforeColumnsList());
            ttj.setOLD_VALUES(t);
        }
        System.out.println(gson.toJson(ttj));
    }

    private static Table createTable(List<Column> cs){
        return new Table(
                String.valueOf(cs.get(0).getValue()),
                cs.get(1).getValue(),
                String.valueOf(cs.get(2).getValue()),
                String.valueOf(cs.get(3).getValue()),
                cs.get(4).getValue(),
                cs.get(5).getValue());
    }

    private static TableToJson createToJson(String database,String table,String binlog,String position){
        return new TableToJson(
                database,
                table,
                "100101",
                binlog,
                position,
                "I",
                "20171109152630",
                "00001",
                null,
                null
        );
    }

}




