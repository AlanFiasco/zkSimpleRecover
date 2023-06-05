package com.recover.util;

public class SnowFlake {
    private static final long START_TIMESTAMP = 1685927619405L;

    private static final int SEQUENCE_BIT = 12;
    private static final int DATACENTER_BIT = 5;
    private static final int MACHINE_BIT = 5;


    private static final long MAX_SEQUENCE = ~(-1L << SEQUENCE_BIT);
    private static final long MAX_DATACENTER_ID = ~(-1L << DATACENTER_BIT);
    private static final long MAX_MACHINE_ID = ~(-1L << MACHINE_BIT);

    // timestamp + dataCenterId + machineId + sequence
    private static final int MACHINE_LEFT = SEQUENCE_BIT;
    private static final int DATA_CENTER_LEFT = MACHINE_LEFT + MACHINE_BIT;
    private static final int TIMESTAMP_LEFT = DATA_CENTER_LEFT + DATACENTER_BIT;


    private final long dataCenterId;
    private final long machineId;
    private long lastTimeStamp = -1L;
    private long sequence = 0L;


    public SnowFlake(long dataCenterId, long machineId) {
        if (dataCenterId > MAX_DATACENTER_ID || dataCenterId <= 0) {
            throw new IllegalArgumentException("dataCenterId must be greater than 0 and less than max_datacenter_id");
        }
        if (machineId > MAX_MACHINE_ID || machineId < 0) {
            throw new IllegalArgumentException("max_datacenter_id must be greater than 0 and less than max_machine_id");
        }
        this.dataCenterId = dataCenterId;
        this.machineId = machineId;
    }

    public synchronized long getNextId() {
        long currentTimeStamp = getTimeStamp();
        if (currentTimeStamp < lastTimeStamp) {
            throw new RuntimeException("Clock moved backwards. Refusing to generate Id");
        }
        if (currentTimeStamp == lastTimeStamp) {
            sequence = (sequence + 1) & MAX_SEQUENCE;
            if (sequence == 0) {
                currentTimeStamp = getNextTimeStamp();
            }
        } else {
            sequence = 0L;
        }
        lastTimeStamp = currentTimeStamp;
        // timestamp + dataCenterId + machineId + sequence
        return (currentTimeStamp - START_TIMESTAMP) << TIMESTAMP_LEFT
                | dataCenterId << DATA_CENTER_LEFT
                | machineId << MACHINE_LEFT
                | sequence;
    }

    private long getNextTimeStamp() {
        long timestamp = getTimeStamp();
        while (timestamp <= lastTimeStamp) {
            timestamp = getTimeStamp();
        }
        return timestamp;
    }

    private long getTimeStamp() {
        return System.currentTimeMillis();
    }

}
