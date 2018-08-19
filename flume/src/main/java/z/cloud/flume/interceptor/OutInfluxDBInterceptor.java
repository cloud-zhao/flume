package z.cloud.flume.interceptor;

import org.apache.flume.Context;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

/**
 * Created by cloud on 17/12/27.
 */
public class OutInfluxDBInterceptor extends BytesCountInterceptor{
    private String influxTable;
    private String dbName;
    private String url;
    private static HttpClient httpClient;

    OutInfluxDBInterceptor(long flushTimeout, Context context){
        super(flushTimeout);
        this.influxTable = context.getString(Constants.TABLE, Constants.DEFAULT_TABLE);
        this.dbName = context.getString(Constants.DB_NAME, Constants.DEFAULT_DB);
        this.url = context.getString(Constants.URL);
        checkState(url != null ,
                "outputType if influx the parameter "+Constants.URL+" must be specified.");
    }

    private HttpClient getHttpClient(){
        if(httpClient == null)
            httpClient = new DefaultHttpClient();
        return httpClient;
    }

    public void flushOutput(){
        try {
            HttpClient httpClient = getHttpClient();
            HttpPost httpPost = new HttpPost(url+"/write?db="+dbName);
            String data = influxTable+",bytes_count=mysql bytes="+bytes+",count="+count;
            bytes=0L;count=0L;
            StringEntity entity = new StringEntity(data);
            entity.setContentEncoding("utf8");
            entity.setContentType("application/x-www-form-urlencoded");
            httpPost.setEntity(entity);
            HttpResponse httpResponse = httpClient.execute(httpPost);
            logger.info("flush output to http. url : "+url+"/write?db="+dbName+" -d "+data);
            if(httpResponse.getStatusLine().getStatusCode() == 204)
                logger.info("flushOutput bytes_count to influxDB successful.");
            else
                logger.error("flushOutput bytes_count to influxDB failed.",httpResponse);
        }catch (Exception e){
            logger.error("flushOutput http failed.",e);
        }
    }
    public void countClose(){
        logger.info("OutInfluxDBInterceptor countClose.");
    }


    private static class Constants {
        private static String TABLE = "influxTable";
        private static String DEFAULT_TABLE = "bytes_count";

        private static String DB_NAME = "influxDB";
        private static String DEFAULT_DB = "cloud_test";

        private static String URL = "influxUrl";
    }
}
