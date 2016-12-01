/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.juniarto.secondsorter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.TextDsi;

/**
 *
 * @author hduser
 */
public class NaturalKeyPartitioner extends Partitioner<TextDsi, IntWritable> {
    public int getPartition(org.apache.hadoop.io.TextDsi key, IntWritable val, int numPartitions){
        int hash = key.getKey().hashCode();
        //int partition = hash % numPartitions;
        //return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        int partition = (hash & Integer.MAX_VALUE) % numPartitions;
        return partition;
    }
}
