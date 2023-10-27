import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class MatrixVectorMultiplication {



    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {

        private IntWritable rowIndex = new IntWritable();

        private DoubleWritable result = new DoubleWritable();

        private double[] vector;



        public void setup(Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();

            String vectorStr = conf.get("vector");

            String[] vectorTokens = vectorStr.split(",");

            vector = new double[vectorTokens.length];

            for (int i = 0; i < vectorTokens.length; i++) {

                vector[i] = Double.parseDouble(vectorTokens[i]);

            }

        }



        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer tokenizer = new StringTokenizer(value.toString(), ",");

            int row = Integer.parseInt(tokenizer.nextToken());

            int col = Integer.parseInt(tokenizer.nextToken());

            double matrixValue = Double.parseDouble(tokenizer.nextToken());



            // Perform the multiplication and emit the result

            double product = matrixValue * vector[col];

            rowIndex.set(row);

            result.set(product);

            context.write(rowIndex, result);

        }

    }



    public static class DoubleSumReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();



        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)

                throws IOException, InterruptedException {

            double sum = 0;

            for (DoubleWritable val : values) {

                sum += val.get();

            }

            result.set(sum);

            context.write(key, result);

        }

    }



    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        conf.set("vector", args[2]); // Pass the vector as a comma-separated string



        Job job = Job.getInstance(conf, "Matrix-Vector Multiplication");



        job.setJarByClass(MatrixVectorMultiplication.class);

        job.setMapperClass(TokenizerMapper.class);

        job.setCombinerClass(DoubleSumReducer.class);

        job.setReducerClass(DoubleSumReducer.class);



        job.setOutputKeyClass(IntWritable.class);

        job.setOutputValueClass(DoubleWritable.class);



        FileInputFormat.addInputPath(job, new Path(args[0])); // Matrix input

        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output



        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}

