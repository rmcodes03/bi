import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;



public class UppercaseWordCount {



    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Text word = new Text();

        private final static IntWritable one = new IntWritable(1);



        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer tokenizer = new StringTokenizer(value.toString());

            while (tokenizer.hasMoreTokens()) {

                String token = tokenizer.nextToken();

                if (token.matches("^[A-Z]+$")) { // Check if the token is all uppercase

                    word.set(token);

                    context.write(word, one);

                }

            }

        }

    }



    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();



        public void reduce(Text key, Iterable<IntWritable> values, Context context)

                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable val : values) {

                sum += val.get();

            }

            result.set(sum);

            context.write(key, result);

        }

    }



    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Uppercase Word Count");



        job.setJarByClass(UppercaseWordCount.class);

        job.setMapperClass(TokenizerMapper.class);

        job.setCombinerClass(IntSumReducer.class);

        job.setReducerClass(IntSumReducer.class);



        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);



        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(args[0]));

        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path(args[1]));



        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}

