package model;

public class LiftRide {
    private Short time;
    private Short liftID;

    public LiftRide(Short time, short liftID) {
        this.time = time;
        this.liftID = liftID;
    }

    public Short getTime() {
        return time;
    }

    public void setTime(Short time) {
        this.time = time;
    }

    public short getLiftID() {
        return liftID;
    }

    public void setLiftID(short liftID) {
        this.liftID = liftID;
    }
}
