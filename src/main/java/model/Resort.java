package model;

public class Resort {
    private String resortName;
    private Integer resortID;

    public Resort(String resortName, Integer resortID) {
        this.resortName = resortName;
        this.resortID = resortID;
    }

    public String getResortName() {
        return resortName;
    }

    public void setResortName(String resortName) {
        this.resortName = resortName;
    }

    public Integer getResortID() {
        return resortID;
    }

    public void setResortID(Integer resortID) {
        this.resortID = resortID;
    }
}
