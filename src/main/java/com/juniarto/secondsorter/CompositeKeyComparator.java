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
public class CompositeKeyComparator extends WritableComparator{
    
    protected CompositeKeyComparator(){
        super(org.apache.hadoop.io.TextDsi.class,true);
    }
    
    public int compare(WritableComparable w1, WritableComparable w2){
        org.apache.hadoop.io.TextDsi k1 = (org.apache.hadoop.io.TextDsi)w1;
        org.apache.hadoop.io.TextDsi k2 = (org.apache.hadoop.io.TextDsi)w2;
        
        int result = k1.getKey().compareTo(k2.getKey());
        if (0 == result){
            result = -1 * ((k1.getOffset() > k2.getOffset() ) ? 1:0);
        }
        return result;
    }
}
