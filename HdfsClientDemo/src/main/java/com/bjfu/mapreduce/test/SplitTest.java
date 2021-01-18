package com.bjfu.mapreduce.test;

import org.junit.Test;

import java.util.Arrays;

public class SplitTest {
    @Test
    public void testSplit() {
        String str = "abc,abc,abc,";
        String[] s = str.split(",");
        System.out.println(s.length);
        System.out.println(Arrays.toString(s));
    }
}
