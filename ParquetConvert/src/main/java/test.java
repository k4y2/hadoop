import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class test {
    public static void main(String[] args) {
        try {
            Date d1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
                    .parse("2016-04-10 19:41:14");
            Date d2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
                    .parse("2016-04-08 17:41:14");
            TimeUnit timeUnit = TimeUnit.MINUTES;
            System.out.println(timeUnit.convert(d1.getTime()-d2.getTime(),TimeUnit.MILLISECONDS));
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
