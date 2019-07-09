package main.java.utils;

public class SlotBasedWindowWeek {
    private long[][] timeframes;
    private int currentIndex;
    private int slot = 12;
    private int day_week = 7;
    private long[] estimatedTotal;

    public SlotBasedWindowWeek() {
        this.timeframes = new long[day_week][slot];

        for (int i = 0; i < day_week; i++) {
            for (int j = 0; j < slot; j++){
                this.timeframes[i][j] = 0;
            }
        }
        this.estimatedTotal = new long[slot];
        for (int k = 0; k < slot; k++) {
            estimatedTotal[k] = 0;
        }
    }

    public void setIndex(long ts) {
        this.currentIndex = DateUtils.getDay(ts);
    }

    public void updateSlot (long timestamp) {
        int i = DateUtils.getSlot(timestamp);
        int day = DateUtils.getDay(timestamp);
        this.timeframes[day][i] = timeframes[day][i] + 1;
        estimatedTotal[i] += 1;
    }

    public long moveForward(){

        /* Free the timeframe that is going to go out of the window */
        int lastTimeframeIndex = (currentIndex + 1) % day_week;
        long value = 0;

        for(int j = 0; j < slot; j++) {
            value = timeframes[lastTimeframeIndex][j];
            timeframes[lastTimeframeIndex][j] = 0;

            estimatedTotal[j] -= value;
        }


        /* Move forward the current index */
        currentIndex = (currentIndex + 1) % day_week;

        return value;

    }

    public int moveForward(int positions){

        int cumulativeValue = 0;

        for (int i = 0; i < positions; i++){

            cumulativeValue += moveForward();

        }

        return cumulativeValue;
    }

    public long[] getEstimatedTotal() {
        return estimatedTotal;
    }

    public long[][] getTimeframes(){
        return this.timeframes;
    }


}
