package main.java.utils;

public class SlotBasedWindow {
    private long[] timeframes;
    private int currentIndex;
    private int size = 12;
    private int estimatedTotal;
    private long timestamp;

    public SlotBasedWindow() {
        this.timeframes = new long[12];
        for (int i = 0; i < size; i++) {
            this.timeframes[i] = 0;
        }

        this.estimatedTotal = 0;
    }

    public void setIndex(long ts) {
        this.currentIndex = DateUtils.getSlot(ts);
    }

    public void updateSlot (long timestamp) {
        int i = DateUtils.getSlot(timestamp);
        this.timeframes[i] = timeframes[i] + 1;
        estimatedTotal += 1;
    }

    public long moveForward(){

        /* Free the timeframe that is going to go out of the window */
        int lastTimeframeIndex = (currentIndex + 1) % size;

        long value = timeframes[lastTimeframeIndex];
        timeframes[lastTimeframeIndex] = 0;

        estimatedTotal -= value;

        /* Move forward the current index */
        currentIndex = (currentIndex + 1) % size;

        return value;

    }

    public int moveForward(int positions){

        int cumulativeValue = 0;

        for (int i = 0; i < positions; i++){

            cumulativeValue += moveForward();

        }

        return cumulativeValue;
    }

    public void increment(){

        increment(1);

    }

    public void increment(int value){

        timeframes[currentIndex]= timeframes[currentIndex] + value;

        estimatedTotal += value;

    }

    @Override
    public String toString() {

        String s = "[";

        for (int i = 0; i < timeframes.length; i++){

            s += timeframes[i];

            if (i < (timeframes.length - 1))
                s += ", ";

        }

        s += "]";
        return s;

    }

    public long[] getTimeframes() {
        return this.timeframes;
    }

    public int getEstimatedTotal() {
        return estimatedTotal;
    }

    public int computeTotal(){

        int total = 0;
        for (int i = 0; i < timeframes.length; i++)
            total += timeframes[i];
        return total;

    }
}
