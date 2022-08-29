package flinkCEP;

public class LoginEvent {
    public String userId;
    public String host;
    public String eventType;
    public Long timesStamp;

    public LoginEvent(){
    }

    public LoginEvent(String userId,String host,String eventType,Long timesStamp){
        this.userId=userId;
        this.host=host;
        this.eventType=eventType;
        this.timesStamp=timesStamp;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
