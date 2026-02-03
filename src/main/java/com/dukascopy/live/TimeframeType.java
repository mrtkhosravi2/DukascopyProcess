package com.dukascopy.live;

public enum TimeframeType {
    S1(1),
    S5(5),
    S10(10),
    S30(30),
    M1(60),
    M3(180),
    M15(900);

    private final int seconds;

    TimeframeType(int seconds) {
        this.seconds = seconds;
    }

    public int getSeconds() {
        return seconds;
    }

    public TimeframeType getParent() {
        switch (this) {
            case S5: return S1;
            case S10: return S5;
            case S30: return S10;
            case M1: return S30;
            case M3: return M1;
            case M15: return M3;
            default: return null;
        }
    }

    public boolean hasSpread() {
        return this == S1;
    }

    public static TimeframeType fromSeconds(int seconds) {
        for (TimeframeType tf : values()) {
            if (tf.seconds == seconds) {
                return tf;
            }
        }
        return null;
    }
}
