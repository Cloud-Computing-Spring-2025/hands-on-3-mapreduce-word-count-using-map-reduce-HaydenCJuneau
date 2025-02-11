package com.example.controller;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


import com.example.WordMapper;
import com.example.WordReducer;

public class Controller {

    public static void main(String[] args) throws IOException {
        JobConf config = new JobConf(Controller.class);

        config.setJobName("WordCount");
        config.setOutputKeyClass(Text.class);
        config.setOutputValueClass(IntWritable.class);
        config.setMapperClass(WordMapper.class);
        config.setCombinerClass(WordReducer.class);
        config.setReducerClass(WordReducer.class);
        config.setInputFormat(TextInputFormat.class);
        config.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(config, new Path(args[0]));
        FileOutputFormat.setOutputPath(config, new Path(args[1]));

        JobClient.runJob(config);
    }
}