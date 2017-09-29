package webHDFS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * Created by hadoop on 2017/6/13
 */
/**
 *  WebHDFS观念是基于HTTP操作，比如GET、PUT、POST和DELETE。
 *  HTTP GET    ：OPEN、GETFILESTATUS、LISTSTATUS
 *  HTTP PUT    : CREAT、MKDIRS、RENAME、SETPERMISSION是
 *  HTTP POST   : APPEND
 *  HTTP DELETE ：DELETE
 *  认证方式可以使用基于user.name参数标准的URL格式如下所示：
 *  http://host:port/webhdfs/v1/?op=operation&user.name=username
 *
 *  1. 列出文件夹下目录
 *  2. 创建文件夹
 *  3. 删除文件夹或者文件
 */
public class HDFSOperation {

    public static void main(String args[]){
        HDFSOperation operation = new HDFSOperation();
        /**
         * 1. 列出文件夹下目录   hadoop fs -ls /user
         */
//        List<String> result = null;
//        try{
//            result = operation.getHDFSDirs("/user");
//        }catch (IOException e){
//            e.printStackTrace();
//        }
//
//        for(String s : result){
//            System.out.println(s);
//        }

        /**
         * 2. 创建文件夹     hadoop fs -mkdir /data
         */
        boolean mkdir = operation.mkdirHDFS("/data1");
        if(mkdir==true){
            System.out.println("make a dir successful!");
        }else{
            System.out.println("make a dir Encounter problems !");
        }
    }

    /**
     * hadoop fs -mkdir /data
     * curl-i -X PUT"http://hadoop-master:14000/webhdfs/v1/tmp/webhdfs?user.name=app&op=MKDIRS"
     * @param dir
     * @return
     */
    public boolean mkdirHDFS(String dir)  {


        String httpfsUrl = BackupUtils.DEFAULT_PROTOCOL;
        String spec = MessageFormat.format("/webhdfs/v1{0}?op=MKDIRS&user.name={1}", dir, "hadoop");
        HttpURLConnection conn = null;
        String resp = "";
        try{
            URL url = new URL(new URL(httpfsUrl), spec);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("PUT");
            conn.connect();
            resp = result(conn, true);
        }catch (IOException e){

            e.printStackTrace();
        }
        conn.disconnect();

        /**
         * 解析返回的json    {"boolean":true}
         */
        JSONObject root = JSON.parseObject(resp);
        boolean flag = root.getBoolean("boolean");

        return flag;
    }

    /**
     * <b>LISTSTATUS</b>
     *
     * curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS&user.name=hdfs"
     * http://192.168.1.23:14000/webhdfs/v1/data?user.name=hadoop&op=MKDIRS
     *
     * @param totalDir
     * @return
     * @throws IOException
     */
    public List<String> getHDFSDirs(String totalDir) throws IOException {
        String httpfsUrl = BackupUtils.DEFAULT_PROTOCOL;
        String spec = MessageFormat.format("/webhdfs/v1{0}?op=LISTSTATUS&user.name={1}", totalDir, "hadoop");
        URL url = new URL(new URL(httpfsUrl), spec);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.connect();
        String resp = result(conn, true);
        conn.disconnect();

        /**
         * 解析json
         * {"FileStatuses":{"FileStatus":[{"pathSuffix":"hadoop","type":"DIRECTORY","length":0,"owner":"hadoop","group":"supergroup","permission":"755","accessTime":0,"modificationTime":1496320647780,"blockSize":0,"replication":0}]}}
         */
        JSONObject root = JSON.parseObject(resp);
        int size = root.getJSONObject("FileStatuses").getJSONArray("FileStatus").size();

        List<String> dirs = new ArrayList<String>();
        for(int i = 0; i < size; ++i) {
            String dir = root.getJSONObject("FileStatuses").getJSONArray("FileStatus").getJSONObject(i).getString("pathSuffix");
            dirs.add(dir);
        }
        System.out.println(resp);
        return dirs;
    }

    /**
     * Report the result in STRING way
     *
     * @param conn
     * @param input
     * @return
     * @throws IOException
     */
    public String result(HttpURLConnection conn, boolean input) throws IOException {
        StringBuffer sb = new StringBuffer();
        if (input) {
            InputStream is = conn.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, "utf-8"));
            String line = null;

            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            reader.close();
            is.close();
        }
        return sb.toString();
    }

    /**
     * delete a dir or file
     * curl -i -X DELETE "http://192.168.1.23:14000/webhdfs/v1/data1?user.name=hadoop&op=DELETE"
     * hadoop fs -rmr /data     hadoop fs -rm /hello
     * @param path
     * @return
     * @throws IOException
     */
    public boolean deleteDirOrFile(String path) throws IOException{

        String httpfsUrl = BackupUtils.DEFAULT_PROTOCOL;
        String spec = MessageFormat.format("/webhdfs/v1{0}?op=DELETE&user.name={1}", path, "hadoop");
        HttpURLConnection conn = null;
        String resp = "";
        try{
            URL url = new URL(new URL(httpfsUrl), spec);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("DELETE");
            conn.connect();
            resp = result(conn, true);
        }catch (IOException e){

            e.printStackTrace();
        }
        conn.disconnect();

        /**
         * 解析返回的json    {"boolean":true}
         */
        JSONObject root = JSON.parseObject(resp);
        boolean flag = root.getBoolean("boolean");

        return flag;

    }
}
