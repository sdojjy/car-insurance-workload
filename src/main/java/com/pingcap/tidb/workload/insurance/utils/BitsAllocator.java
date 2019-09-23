package com.pingcap.tidb.workload.insurance.utils;

public class BitsAllocator {
    public static final int TOTAL_BITS = 64;
    private int signBits = 1;
    private final int timestampBits;
    private final int workerIdBits;
    private final int sequenceBits;
    private final long maxDeltaSeconds;
    private final long maxWorkerId;
    private final long maxSequence;
    private final int timestampShift;
    private final int workerIdShift;

    public BitsAllocator(int timestampBits, int workerIdBits, int sequenceBits) {
        int allocateTotalBits = this.signBits + timestampBits + workerIdBits + sequenceBits;
       // Assert.isTrue(allocateTotalBits == 64, "allocate not enough 64 bits");
        this.timestampBits = timestampBits;
        this.workerIdBits = workerIdBits;
        this.sequenceBits = sequenceBits;
        this.maxDeltaSeconds = ~(-1L << timestampBits);
        this.maxWorkerId = ~(-1L << workerIdBits);
        this.maxSequence = ~(-1L << sequenceBits);
        this.timestampShift = workerIdBits + sequenceBits;
        this.workerIdShift = sequenceBits;
    }

    public long allocate(long deltaSeconds, long workerId, long sequence) {
        return deltaSeconds << this.timestampShift | workerId << this.workerIdShift | sequence;
    }

    public int getSignBits() {
        return this.signBits;
    }

    public int getTimestampBits() {
        return this.timestampBits;
    }

    public int getWorkerIdBits() {
        return this.workerIdBits;
    }

    public int getSequenceBits() {
        return this.sequenceBits;
    }

    public long getMaxDeltaSeconds() {
        return this.maxDeltaSeconds;
    }

    public long getMaxWorkerId() {
        return this.maxWorkerId;
    }

    public long getMaxSequence() {
        return this.maxSequence;
    }

    public int getTimestampShift() {
        return this.timestampShift;
    }

    public int getWorkerIdShift() {
        return this.workerIdShift;
    }

//    public String toString() {
//        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
//    }
}
