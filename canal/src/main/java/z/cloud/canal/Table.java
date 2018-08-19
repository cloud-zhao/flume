package z.cloud.canal;

/**
 * Created by cloud on 17/11/9.
 */
public class Table {
    private String id = "";
    private String name = "";
    private String age = "";
    private String height = "";
    private String des = "";
    private String timestamp = "";

    public Table(String id,
                 String name,
                 String age,
                 String height,
                 String des,
                 String timestamp){
        this.id=id;
        this.name=name;
        this.age=age;
        this.height=height;
        this.des=des;
        this.timestamp=timestamp;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public void setHeight(String height) {
        this.height = height;
    }

    public void setDes(String des) {
        this.des = des;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getId() {

        return id;
    }

    public String getName() {
        return name;
    }

    public String getAge() {
        return age;
    }

    public String getHeight() {
        return height;
    }

    public String getDes() {
        return des;
    }

    public String getTimestamp() {
        return timestamp;
    }
}
