/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.juniarto.secondsorter;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import org.apache.hadoop.io.TextDsi;
import org.zeromq.ZMQ;



/**
 *
 * @author hduser
 */
public class SsJob extends Configured implements Tool{
    private static final Log LOG = LogFactory.getLog(SsJob.class.getName());

    
    
    public static class SsMapper extends Mapper<LongWritable, Text, org.apache.hadoop.io.TextDsi, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private org.apache.hadoop.io.TextDsi wordDsi;
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            int i = 1;
            while (tokenizer.hasMoreTokens()){
                word.set(tokenizer.nextToken());
                //LOG.info("AttemptID: " + context.getTaskAttemptID().getId());
                if (context.getTaskAttemptID().getId() == 0){
                    if (word.toString().equals("juniarto")){
                        
                        try {
                            //RuntimeException e = new RuntimeException();
                            //throw e;
                            killMap(context.getTaskAttemptID());
                            //Thread.sleep(1);
                        } catch (Exception ex) {
                            Logger.getLogger(SsJob.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                }
                //LOG.info("SSJOB: OFFSET: " + key.get() + " WORD: "+ word.toString() + " KEYCOUNT: " + i );
                wordDsi = new org.apache.hadoop.io.TextDsi(word.toString(),key.get(),i);
                //LOG.info("SSJOB: THEKEYCOUNT: " + wordDsi.getKeyCount() + " " + wordDsi.toString());
                context.write(wordDsi,one);
                i++;
                //Thread.sleep(2);
            }
            //LOG.info("SSJOB TEXT: " + value.toString() + "KEYCOUNT: " + (i-1));
            
        }
    }
    
    public static class SsReducer extends Reducer<org.apache.hadoop.io.TextDsi, IntWritable, Text, IntWritable>{
        @Override
        public void reduce(org.apache.hadoop.io.TextDsi key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            Text k = new Text(key.getKey());
            int sum = 0;
            for (IntWritable val : values){
                sum += val.get();
            }
            context.write(k, new IntWritable(sum));
        }
    }

    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        //long time1= System.nanoTime();
        ToolRunner.run(new Configuration(), new SsJob(), args);
        //long time2 = System.nanoTime();
        //long timeSpent = time2-time1;
        //LOG.info("TIME: " + timeSpent);
    }
    
    public static void killMap(TaskAttemptID taskAttemptID) throws Exception{

        //Job job = new Job(conf,"secondary sort");
        //job.failTask(taskAttemptID);
        //LOG.info("mapred job -kill-task " + taskAttemptID);
        //Process p = Runtime.getRuntime().exec("/home/hduser/hadoop-2.7.1-src/hadoop-dist/target/hadoop-2.7.1/bin/mapred job -fail-task " + taskAttemptID);
        //p.waitFor();
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket requester = context.socket(ZMQ.REQ);
        requester.connect("tcp://localhost:5557");
        
        String attemptID = taskAttemptID.toString();
        requester.send(attemptID.getBytes(),0);
        byte[] reply = requester.recv(0);
        //LOG.info("RECEIVED " + new String(reply));
        requester.close();
        context.term();
        
    }
    
    public Configuration getConf2(){
        Configuration conf = getConf();
        return conf;
    }
       
   public int run(String[] allArgs) throws Exception{
       Configuration conf = getConf();
       Job job = new Job(conf,"secondary sort");
      
       job.setJarByClass(SsJob.class);
       job.setPartitionerClass(NaturalKeyPartitioner.class);
       job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
       job.setSortComparatorClass(CompositeKeyComparator.class);
       
       job.setMapOutputKeyClass(TextDsi.class);
       job.setMapOutputValueClass(IntWritable.class);
       
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(IntWritable.class);
       
       job.setInputFormatClass(TextInputFormat.class);
       job.setOutputFormatClass(TextOutputFormat.class);
       
       job.setMapperClass(SsMapper.class);
       job.setReducerClass(SsReducer.class);
       job.setNumReduceTasks(2);
       
       String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
       FileInputFormat.setInputPaths(job, new Path(args[0]));
       FileOutputFormat.setOutputPath(job, new Path(args[1]));
       //job.submit();
       
       long time1 = System.nanoTime();
       boolean status = job.waitForCompletion(true);
       long time2 = System.nanoTime();
       long timeSpent = time2-time1;
       LOG.info("TIME: " + timeSpent);
       return 0;
       
   }
    
}
