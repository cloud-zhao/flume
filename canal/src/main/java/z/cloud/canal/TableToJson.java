package z.cloud.canal;

import java.util.Random;

/**
 * Created by cloud on 17/11/9.
 */
public class TableToJson{
    private String DATABASE = "";
    private String GLOBAL_ID = "";
    private String EVENT_SERVER_ID = "";
    private String BINLOG_NAME = "";
    private String BINLOG_POS = "";
    private String TIME = "";
    private String TABLE = "";
    private String GROUP_ID = "";
    private String TYPE = "";
    private Object NEW_VALUES=null;
    private Object OLD_VALUES=null;

    public TableToJson(String DATABASE,
                       String TABLE,
                       String EVENT_SERVER_ID,
                       String BINLOG_NAME,
                       String BINLOG_POS,
                       String TYPE,
                       String TIME,
                       String GROUP_ID,
                       Object NEW_VALUES,
                       Object OLD_VALUES){
        this.DATABASE=DATABASE;
        this.TABLE=TABLE;
        this.EVENT_SERVER_ID=EVENT_SERVER_ID;
        this.BINLOG_NAME=BINLOG_NAME;
        this.BINLOG_POS=BINLOG_POS;
        this.TYPE=TYPE;
        this.TIME=TIME;
        this.GROUP_ID=GROUP_ID;
        this.NEW_VALUES=NEW_VALUES;
        this.OLD_VALUES=OLD_VALUES;
        setGLOBAL_ID();
    }

    public String getDATABASE() {
        return DATABASE;
    }

    public String getGLOBAL_ID() {
        return GLOBAL_ID;
    }

    public String getEVENT_SERVER_ID() {
        return EVENT_SERVER_ID;
    }

    public String getBINLOG_NAME() {
        return BINLOG_NAME;
    }

    public String getBINLOG_POS() {
        return BINLOG_POS;
    }

    public String getTIME() {
        return TIME;
    }

    public String getTABLE() {
        return TABLE;
    }

    public String getGROUP_ID() {
        return GROUP_ID;
    }

    public String getTYPE() {
        return TYPE;
    }

    public Object getNEW_VALUES() {
        return NEW_VALUES;
    }

    public Object getOLD_VALUES() {
        return OLD_VALUES;
    }

    public void setGLOBAL_ID() {

        this.GLOBAL_ID = this.DATABASE+"-"+this.TABLE+"-0-"+getGlobalId()+"-0";
    }

    public void setEVENT_SERVER_ID(String EVENT_SERVER_ID) {
        this.EVENT_SERVER_ID = EVENT_SERVER_ID;
    }

    public void setBINLOG_NAME(String BINLOG_NAME) {
        this.BINLOG_NAME = BINLOG_NAME;
    }

    public void setBINLOG_POS(String BINLOG_POS) {
        this.BINLOG_POS = BINLOG_POS;
    }

    public void setTIME(String TIME) {
        this.TIME = TIME;
    }

    public void setTABLE(String TABLE) {
        this.TABLE = TABLE;
    }

    public void setGROUP_ID(String GROUP_ID) {
        this.GROUP_ID = GROUP_ID;
    }

    public void setTYPE(String TYPE) {
        this.TYPE = TYPE;
    }

    public void setNEW_VALUES(Object NEW_VALUES) {
        this.NEW_VALUES = NEW_VALUES;
    }

    public void setOLD_VALUES(Object OLD_VALUES) {
        this.OLD_VALUES = OLD_VALUES;
    }

    public void setDATABASE(String DATABASE) {

        this.DATABASE = DATABASE;
    }

    public static String getGlobalId(){
        int max=999999999;
        int min=100000000;
        Random random = new Random();

        int s = random.nextInt(max)%(max-min+1) + min;
        return String.valueOf(s);
    }
}
