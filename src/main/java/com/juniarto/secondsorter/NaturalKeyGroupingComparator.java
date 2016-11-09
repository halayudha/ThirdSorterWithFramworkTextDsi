/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.juniarto.secondsorter;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.TextDsi;
/**
 *
 * @author hduser
 */
public class NaturalKeyGroupingComparator extends WritableComparator {
    
    protected NaturalKeyGroupingComparator(){
        super(org.apache.hadoop.io.TextDsi.class, true);
    }
    
    public int compare(WritableComparable w1, WritableComparable w2){
        org.apache.hadoop.io.TextDsi k1 = (org.apache.hadoop.io.TextDsi)w1;
        org.apache.hadoop.io.TextDsi k2 = (org.apache.hadoop.io.TextDsi)w2;
        
        return k1.getKey().compareTo(k2.getKey());
    }
    
}
