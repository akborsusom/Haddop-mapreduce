import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
/*
* Here query 1 refers to 'MRSQL hdfs://localhost/data/Batting.csv --filter 11 60 --project 0'
* Here query 2 refers to 'MRSQL hdfs://localhost/data/Batting.csv --join hdfs://localhost/data/People.csv 0 0 --filter 1 1997 --group 0 --having 11 50 --project 36'
* */

public class MRSQL {

    // Here starting the MRSQL class's main method
    public static void main(String[] args) throws Exception {
        if(args.length == 5) {
            /* As a command line input to run query 2, five parameters are executed below.
             * @Pram Batting.csv, People.csv, year_id, sum of hr, output path
             */
            Configuration conf = new Configuration();
            conf.set("playing_year", args[2]);
            conf.set("hr", args[3]);
            Job job = new Job(conf, "query 2");
            job.setJarByClass(ReduceJoinReducer.class);
            job.setReducerClass(ReduceJoinReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustsMapper.class);
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TxnsMapper.class);
            FileOutputFormat.setOutputPath(job, new Path(args[4]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }else if(args.length == 3){
            /* Three parameters are executed below, required for command line input to run query 1.
             * @Pram Batting.csv, hr, output path
             */
            Configuration conf = new Configuration();
            conf.set("given_hr_value", args[1]);
            Job job = Job.getInstance(conf, "query 1");
            job.setJarByClass(MRSQL.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }


    // This is a mapper class for extracting data from Batting.csv for query 2
    public static class CustsMapper extends Mapper<Object, Text, Text, Text> {

        private String playing_year = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Assign provided playing_year value as global variable
            playing_year = context.getConfiguration().get("playing_year");

        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");

            String yearAndHr = parts[11];
            if (parts[1].equals(playing_year)) {
                context.write(new Text(parts[0]), new Text("Batting\t" + yearAndHr));
            }


        }
    }

    // This is a mapper class for extracting data from People.csv for query 2
    public static class TxnsMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");
            // parts[14] contains the lastname of People.csv file
            String lastName = parts[14];
            context.write(new Text(parts[0]), new Text("People\t" + lastName));

        }
    }

    //  Reducer class is implemented here to execute logic for query 2
    public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {

        String hr = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Assign provided hr value as global variable
            hr = context.getConfiguration().get("hr");
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double sumOfHr = 0.0;
            String name = "";
            String resultWithHrSum = "";
            // Obtaining a single hr and name value from the preceding list
            for (Text value : values) {
                String[] battingAndPeople = value.toString().split("\t");
                if (battingAndPeople[0].equals("Batting")) {
                    // sumOfHr variable contains hr value sum of Batting.csv file
                    sumOfHr = sumOfHr + Double.valueOf(battingAndPeople[1]);
                } else if (battingAndPeople[0].equals("People")) {
                    name = battingAndPeople[1];
                }

            }
            if (sumOfHr > Integer.valueOf(hr)) {
                resultWithHrSum = name;
                context.write(new Text(resultWithHrSum), new Text());

            }

        }
    }

    // Mapper class is implemented here for extracting  data from Batting.csv for query 1
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private String given_hr_value = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Assigning the hr value given as a global variable
            given_hr_value = context.getConfiguration().get("given_hr_value");

        }

        private Text playerId = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] record = line.substring(0, line.length() - 1).split(",");

            // Here record[11] contains hr value of dataset
            // Checking if the condition is equal to the given hr value or not.
            if (record[11].equals(given_hr_value)) {
                playerId.set(record[0]);
                context.write(playerId, new Text(record[11]));
            }

        }
    }

    // This is the Reducer class that will execute the logic for query 1.
    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(key, new Text());

            }


        }
    }
}