import java.text.DecimalFormat;
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

public class InvertedIndex_TFIDF {

        public static class Map extends Mapper<Object, Text, Text, Text> {
        private Text keyInfo = new Text();
        private Text valueInfo = new Text();
        private FileSplit split;     //获取文件名
        private String pattern = "[^a-zA-Z0-9-]";   //统计词频时去掉标点符号，定义表达式

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            split = (FileSplit) context.getInputSplit();
            String line = value.toString();      //将每一行转化为string
            line = line.replaceAll(pattern, " ");     //用空格替换标点符号
            String[] words = line.split("\\s+");      //将string转化为一个个单词
            DecimalFormat df = new DecimalFormat("0.00"); //规定float格式
            String num = df.format((float)1/words.length);  //计算词频率

                for (String word : words)  {
                if (word.length() > 0) {
                    keyInfo.set(word +":"+split.getPath().getName().toString());  //设置键值为单词加上>文件名 如"Mapper:a.txt"

                    valueInfo.set(num);        //设置词频率
                    context.write(keyInfo, valueInfo);
                }
            }
        }
    }

    public static class Combine extends Reducer<Text, Text, Text, Text> {
        private Text info = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //统计单个文件词频率
            float sum = 0;
            for (Text value : values) {
                sum += Float.parseFloat(value.toString());
            }
            //将key设置为单词，而value表示文件名以及对应频数，如"Mapper: (a.txt,1)"
            int splitIndex = key.toString().indexOf(":");
            info.set(key.toString().substring(splitIndex + 1) + "," + sum);
            key.set(key.toString().substring(0, splitIndex));
            context.write(key, info);
        }
    }

        public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //相同的单词进行归约，统计某单词在不同文件中对应频数
            String fileList = new String();
            int count = 0;
            for (Text value : values) {
                fileList += "(" + value.toString() + ")" + " ";
                count++;
            }
            //在value值中输出IDF
            double num = Math.log((double)2/count);
            fileList += "( idf ," + num + " )";
            result.set(fileList);
            context.write(key, result);
        }
    }
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //相同的单词进行归约，统计某单词在不同文件中对应频数
            String fileList = new String();
            int count = 0;
            for (Text value : values) {
                fileList += "(" + value.toString() + ")" + " ";
                count++;
            }
            double num = Math.log((double)2/count);
            fileList += "( idf ," + num + " )";
            result.set(fileList);
            context.write(key, result);
        }
    }

        public static void main(String[] args) throws Exception {
        //创建配置对象
        Configuration conf = new Configuration();
        // 创建Job对象
        Job job = Job.getInstance(conf, "invertedIndex_TFIDF");
        job.setJarByClass(InvertedIndex.class);
        //设置job Mapper Reduce类
        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
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
                System.out.println("Wordcount task fail!");
        }
    }
}
