import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class test {
    public static void main(String[] args) {
        try {
            Date d1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
                    .parse("2016-10-06 00:05:09");
            Date d2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
                    .parse("2016-10-05 23:58:56");
            System.out.println(d2.toString());
            System.out.println(d1.getTime()-d2.getTime());
            TimeUnit timeUnit = TimeUnit.SECONDS;
            System.out.println((d1.getTime()-d2.getTime())/1000%60);
            System.out.println(timeUnit.convert(d1.getTime()-d2.getTime(),TimeUnit.MILLISECONDS));
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
