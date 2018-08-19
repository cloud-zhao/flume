package z.cloud.canal;

import com.google.gson.Gson;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * Created by cloud on 17/11/9.
 */
public class Test {
    public static void main(String[] args) throws ClassNotFoundException,NoSuchMethodException,
            InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
            SecurityException  {
        System.out.println("20000000");
        /*Gson gson=new Gson();

        Map<String,Object> kv = new HashMap<String,Object>();
        kv.put("abc","200");
        kv.put("a1","20");
        kv.put("a2","00");

        Map<String,String> subkv = new HashMap<String, String>();
        subkv.put("s1","[2000,200,300]");
        subkv.put("s2","20.20,39,20");

        kv.put("sub",subkv);

        p1(kv);*/

        /*T2 t2o=new T2();

        for(int i=0;i<10;i++){
            System.out.println(t2o.t2(kv,"k"+String.valueOf(i)));
        }*/

   /*     kv.remove("Not_Exists_Key");

        T1 t1=new T1("4000");
        Tt1 tt1=new Tt1("abcdefg",t1);

        System.out.println(gson.toJson(tt1));

        System.out.println("Map to json : "+gson.toJson(kv));*/

/*        Map<String,Object> tFParameters = new HashMap<String, Object>();
        tFParameters.put("sf1","200000000");
        tFParameters.put("sf2","088882");
        tFParameters.put("sf3","jie");

        Object tF = CreateTFactory.create("z.cloud.test.Tf1",tFParameters);

        System.out.println(gson.toJson(tF));

        Clazz clazz = new Clazz("z.cloud.test.T1");
        clazz.parseClass();*/

        Tss tss = new TssA();
        //tss.setName();
        tss.getName();

    }

    private static void p1(Map<String,Object> p){
        String pp1 = (String)p.get("abc");
        System.out.println("p1 -> pp1 : "+pp1);
    }
}

abstract class Tss {
    private String v="200000";
    protected String n="abc";

    abstract void setName();
    void getName(){
        System.out.println("v : " + v);
        setName();
    }
}

class TssA extends Tss {
    public void setName(){
        System.out.println(n);
        n="foo";
        System.out.println(n);
    }
}

class Clazz {
    private String className;
    Class<?> clazz;
    public Clazz(String className){
        this.className=className;
    }

    public void parseClass() throws ClassNotFoundException,NoSuchMethodException,
            InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
            SecurityException {
        clazz = Class.forName(className);
        Constructor<?>[] constructors = clazz.getConstructors();

        System.out.println("Constructor length : "+constructors.length);
        for (int i=0;i<constructors.length;i++){
            System.out.println("constructor : "+constructors[i].toString());
            parseParameter(constructors[i]);
        }
    }

    public void parseParameter(Constructor<?> constructor) throws ClassNotFoundException,NoSuchMethodException,
            InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
            SecurityException
    {
        Class[] parameterTypes = constructor.getParameterTypes();

        System.out.println("\tparameterType length : "+parameterTypes.length);
        for(int i=0;i<parameterTypes.length;i++){
            System.out.println("\t\tparameter type : "+parameterTypes[i].toString());
        }
    }
}

class CreateTFactory{
    public static Object create(String className,Map<String,Object> parameters){
        Object tFObject=null;
        try {
            Class<?> clazz = Class.forName(className);
            tFObject = clazz.newInstance();
            Field[] fields = clazz.getDeclaredFields();
            //System.out.println("fields length : "+fields.length);
            for(int i=0;i<fields.length;i++){
               /* System.out.println(fields[i].getName());*/
                fields[i].setAccessible(true);
                fields[i].set(tFObject,parameters.get(fields[i].getName()));
            }
        }catch (Exception e){
            System.out.println("Exception : "+e.toString());
        }
        return tFObject;
    }
}

class T2 {
    private Gson gson = new Gson();
    public String t2(Map<String,Object> hm,String k){
        hm.put(k,"t2");
        return gson.toJson(hm);
    }
}

interface TFactory{
    //null
}

class Tf1 implements TFactory{
    private String sf1;
    private String sf2;
    private String sf3;

    public Tf1(){}

    public Tf1(String sf1, String sf2, String sf3) {
        this.sf1 = sf1;
        this.sf2 = sf2;
        this.sf3 = sf3;
    }

    public void setSf1(String sf1) {
        this.sf1 = sf1;
    }

    public void setSf2(String sf2) {
        this.sf2 = sf2;
    }

    public void setSf3(String sf3) {
        this.sf3 = sf3;
    }
}

class T1{
    private String t1="";
    private int t2;
    public T1(String t1,int t2) {
        this.t1=t1;
        this.t2=t2;
    }
    public T1(String t1){
        this(t1,200);
    }
    public T1(int t2){
        this("200000",t2);
    }
    public T1(){
        this("2000");
    }

    public void setT1(String t1) {
        this.t1 = t1;
    }

    public String getT1() {

        return t1;
    }
    public String getAll(){
        return t1+"___"+String.valueOf(t2);
    }
}

class Tt1{
    private String ttname="";
    private Object tt=null;

    public Tt1(String ttname,Object tt){
        this.ttname=ttname;
        this.tt=tt;
    }

    public void setTtname(String ttname) {
        this.ttname = ttname;
    }

    public void setTt(Object tt) {
        this.tt = tt;
    }

    public String getTtname() {

        return ttname;
    }

    public Object getTt() {
        return tt;
    }
}

class Tu{
    public static int findNext(String str, char separator, char escapeChar, int start, StringBuilder split) {
        int numPreEscapes = 0;

        for(int i = start; i < str.length(); ++i) {
            char curChar = str.charAt(i);
            if(numPreEscapes == 0 && curChar == separator) {
                return i;
            }

            split.append(curChar);
            int var10000;
            if(curChar == escapeChar) {
                ++numPreEscapes;
                var10000 = numPreEscapes % 2;
            } else {
                var10000 = 0;
            }

            numPreEscapes = var10000;
        }

        return -1;
    }
}