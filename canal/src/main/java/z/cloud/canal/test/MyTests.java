package z.cloud.canal.test;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

/**
 * Created by cloud on 17/12/27.
 */
public class MyTests {

    public static void main(String[] args){
        MyTests myTests = new MyTests();
        myTests.t1(10L,20L);
    }

    private void t1(long bytes,long count) {

        String url = "http://data-spark-8-114:8086/write?db=cloud_test";

        try {
            HttpClient httpClient = new DefaultHttpClient();
            HttpPost httpPost = new HttpPost(url);
            String data = "mysql_bytes_count,bytes_count=mysql bytes=" + bytes + ",count=" + count;
            System.out.println(url + " -d " + data);
            StringEntity entity = new StringEntity(data);
            entity.setContentEncoding("utf8");
            entity.setContentType("application/x-www-form-urlencoded");
            httpPost.setEntity(entity);
            HttpResponse httpResponse = httpClient.execute(httpPost);
            System.out.println(httpResponse);
        } catch (Exception e) {
            System.out.println("flushOutput http failed. "+e);
        }
    }
}
