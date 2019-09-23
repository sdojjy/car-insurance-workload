package com.pingcap.tidb.workload.insurance.utils;

import java.util.concurrent.TimeUnit;

public class UidGenerator {

    private  int timeBits = 28;
    private  int workerBits = 22;
    private  int seqBits = 13;

    private long epochSeconds;
    private BitsAllocator bitsAllocator;
    private long workerId;
    private long sequence;
    private long lastSecond;

    public UidGenerator(int timeBits, int workerBits, int seqBits) {
        this.timeBits = timeBits;
        this.workerBits = workerBits;
        this.seqBits = seqBits;
        this.bitsAllocator = new BitsAllocator(timeBits, workerBits, seqBits);
        this.epochSeconds = TimeUnit.MILLISECONDS.toSeconds(1463673600000L);
        this.sequence = 0L;
        this.lastSecond = -1L;
    }


    public long getUID() throws UidGenerateException {
        try {
            return this.nextId();
        } catch (Exception var2) {
            throw new UidGenerateException(var2);
        }
    }


    protected synchronized long nextId() {
        long currentSecond = this.getCurrentSecond();
        if (currentSecond < this.lastSecond) {
            long refusedSeconds = this.lastSecond - currentSecond;
            throw new UidGenerateException("Clock moved backwards. Refusing for %d seconds",
                new Object[]{refusedSeconds});
        } else {
            if (currentSecond == this.lastSecond) {
                this.sequence = this.sequence + 1L & this.bitsAllocator.getMaxSequence();
                if (this.sequence == 0L) {
                    currentSecond = this.getNextSecond(this.lastSecond);
                }
            } else {
                this.sequence = 0L;
            }

            this.lastSecond = currentSecond;
            return this.bitsAllocator
                .allocate(currentSecond - this.epochSeconds, this.workerId, this.sequence);
        }
    }

    private long getNextSecond(long lastTimestamp) {
        long timestamp;
        for (timestamp = this.getCurrentSecond(); timestamp <= lastTimestamp;
            timestamp = this.getCurrentSecond()) {
        }

        return timestamp;
    }

    private long getCurrentSecond() {
        long currentSecond = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        if (currentSecond - this.epochSeconds > this.bitsAllocator.getMaxDeltaSeconds()) {
            throw new UidGenerateException(
                "Timestamp bits is exhausted. Refusing UID generate. Now: " + currentSecond);
        } else {
            return currentSecond;
        }
    }

    public void setEpochSeconds(long epochSeconds) {
        this.epochSeconds = epochSeconds;
    }

    public void setWorkerId(long workerId) {
        this.workerId = workerId;
    }
}
