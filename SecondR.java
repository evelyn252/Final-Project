import java.util.ArrayList;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SecondR {

        public static class Map extends Mapper<Object, Text, Text, Text> {
        private Text keyInfo = new Text();
        private Text valueInfo = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();      //将每一行转化为string
            String[] words = line.split("\\s+");      //将string转化为一个个单词

            keyInfo.set(words[0]);            //每一行设置两个key-value组
            valueInfo.set(words[1]); 
            context.write(keyInfo, valueInfo);
            keyInfo.set(words[1]);
            valueInfo.set(words[0]);
            context.write(keyInfo, valueInfo);
        }
    }

public static class Reduce extends Reducer<Text, Text, Text, Text> {
    private Text f1 = new Text();
    private Text f2 = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<String> list =new ArrayList<String>();
        for (Text value : values) {     //统计同一个的相关朋友
            list.add(value.toString());
        }
        for(int i=0;i<list.size();i++){    //对这些相关朋友进行两两组合
            f1.set(list.get(i));
            for(int j=i+1;j<list.size();j++){
                f2.set(list.get(j));
                context.write(f1,f2);
            }
        }
    }
}

    public static void main(String[] args) throws Exception {
    //创建配置对象
    Configuration conf = new Configuration();
    // 创建Job对象
    Job job = Job.getInstance(conf, "Second-degree-relation");
    job.setJarByClass(SecondR.class);
    //设置job Mapper Reduce类
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    // 设置Map输出的Key value
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    // 设置Reduce输出的Key value
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    // 设置输入输出的路径
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    // 提交job
    boolean b = job.waitForCompletion(true);
    if(!b) {
            System.out.println("The task fail!");
    }
}
}

