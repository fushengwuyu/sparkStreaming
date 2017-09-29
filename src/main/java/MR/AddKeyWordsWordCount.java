package MR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by hadoop on 2017/6/22.
 *
 * ：根据命令行参数统计输入文件中指定关键字出现的次数，并展示出来
 *  例如：hadoop jar xxxxx.jar keywordcount xxx,xxx,xxx,xxx(四个关键字）,在结果中只显示关键字出现的次数。
 *
 *  分析：只是替换分隔符从空格到逗号，以及增加搜索关键字列表：在Map中增加一个关键字集合，在主函数中为关键字集合添加数据即可。
 */
public class AddKeyWordsWordCount {

    public static class KeyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        private final static ArrayList<String> keywords = new ArrayList<String>();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(" ");
            for(String s: line){
                if(keywords.contains(s)){
                    context.write(new Text(s), new IntWritable(1));
                }
            }

        }

        public static void keyAdd(String item){
            keywords.add(item);
        }
    }

    public static class keyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable i: values){
                sum += i.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length < 3){
            System.out.println("args error");
            System.exit(2);
        }
        String[] target_words = args[2].split(",");
        for (String word : target_words) {
            KeyMapper.keyAdd(word.toLowerCase());
        }
        Job job = Job.getInstance(conf);
        job.setJarByClass(AddKeyWordsWordCount.class);
        job.setMapperClass(KeyMapper.class);
        job.setReducerClass(keyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
