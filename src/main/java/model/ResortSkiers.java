package model;

public class ResortSkiers {
    private String time;
    private Integer numSkiers;

    public ResortSkiers(String time, Integer numSkiers) {
        this.time = time;
        this.numSkiers = numSkiers;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public Integer getNumSkiers() {
        return numSkiers;
    }

    public void setNumSkiers(Integer numSkiers) {
        this.numSkiers = numSkiers;
    }
}
