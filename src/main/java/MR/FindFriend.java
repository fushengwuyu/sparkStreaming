package MR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


import java.io.IOException;

import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

/**
 * Created by hadoop on 2017/6/21.
 *
 * mapred 找共同朋友，数据格式如下
 * 1. A B C D E F
 * 2. B A C D E
 * 3. C A B E
 * 4. D A B E
 * 5. E A B C D
 * 6. F A
 * 第一字母表示本人，其他是他的朋友，找出有共同朋友的人，和共同朋友是谁
 * 分析： 对A，其朋友b,c,d,e,f 任意两两组合的共同朋友都是A，同理，对任意R，其朋友中任意两两组合的共同朋友都是R。
 *      所以，我们将R朋友的两两组合作为map 的key，R本身为value。
 *
 */
public class FindFriend {
    public static class FindMapper extends Mapper<Object, Text, Text, Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            Text owner = new Text();
            Set<String> set = new TreeSet<String>();
            owner.set(itr.nextToken());
            while(itr.hasMoreTokens()){
                set.add(itr.nextToken());
            }
            String[] friend = new String[set.size()];
            friend = set.toArray(friend);
            for(int i=0;i<friend.length;i++){
                for(int j=i+1;j<friend.length;j++){
                    String outputkey = friend[i]+friend[j];
                    context.write(new Text(outputkey), owner);
                }
            }
        }
    }

    public static class FriendReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           String commonfriends = "";
           for(Text t: values){
               if(commonfriends == ""){
                   commonfriends = t.toString();
               }
               else{
                   commonfriends = commonfriends + t.toString();
               }
           }
           context.write(key, new Text(commonfriends));
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length < 2){
            System.out.println("args error");
            System.exit(2);
        }
        Job job = Job.getInstance(conf);
        job.setJarByClass(FindFriend.class);
        job.setMapperClass(FindMapper.class);
        job.setCombinerClass(FriendReducer.class);
        job.setReducerClass(FriendReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }

}
