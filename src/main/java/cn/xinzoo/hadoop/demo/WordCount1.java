package cn.xinzoo.hadoop.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class WordCount1 {

    public static class  WordCountMapper extends Mapper<Object, Text, Text, IntWritable>{
        private final  static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value,  Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (String s : words) {
                word.set(s);
                context.write(word,one);
            }
        }
    }
    public static  class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public  void reduce(Text key,Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)throws IOException, InterruptedException {
            int total = 0;
            //values.iterator().
            for (IntWritable val : values) {
                total ++ ;
            }
            context.write(key,new IntWritable(total));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount1.class);
        job.setMapperClass(WordCountMapper.class);
        //job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        for(int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
