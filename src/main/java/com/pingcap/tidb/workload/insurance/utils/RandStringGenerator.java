package com.pingcap.tidb.workload.insurance.utils;

public class RandStringGenerator {

    private static char[] a = "ABCDEFGHIJKLMNOPSRSTUVWXYZabcdefghigklmnopsrstuvwxyz0123456789"
        .toCharArray();
    private static int byteLen = a.length;
    private Pcg32 rnd = new Pcg32();

    public String genRandStr(int size) {
        char[] buf = new char[size];
        for (int i = 0; i < size; i++) {
            buf[i]= a[rnd.nextInt(byteLen)];
        }
        return new String(buf);
    }
}
