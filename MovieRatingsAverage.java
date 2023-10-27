import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;



public class MovieRatingsAverage {



    public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text movieId = new Text();

        private DoubleWritable rating = new DoubleWritable();



        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");

            if (fields.length == 4) {

                movieId.set(fields[0]);

                rating.set(Double.parseDouble(fields[2]));

                context.write(movieId, rating);

            }

        }

    }



    public static class DoubleSumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();



        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)

                throws IOException, InterruptedException {

            double sum = 0;

            int count = 0;

            for (DoubleWritable val : values) {

                sum += val.get();

                count++;

            }

            double average = sum / count;

            result.set(average);

            context.write(key, result);

        }

    }



    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Movie Ratings Average");



        job.setJarByClass(MovieRatingsAverage.class);

        job.setMapperClass(TokenizerMapper.class);

        job.setCombinerClass(DoubleSumReducer.class);

        job.setReducerClass(DoubleSumReducer.class);



        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(DoubleWritable.class);



        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(args[0]));

        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path(args[1]));



        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}

