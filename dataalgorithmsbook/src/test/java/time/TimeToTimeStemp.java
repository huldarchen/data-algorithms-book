package time;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Scanner;

/**
 * TODO
 *
 * @author huldar
 * @date 2019/4/29 12:36
 */
public class TimeToTimeStemp {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        boolean flag = true;
        LocalDateTime now = LocalDateTime.now();
        while (flag) {
            System.out.println("请输入 日期 日期格式是 yyyy-MM-dd HH:mm:ss,退出请输入exit");
            String dateString = scanner.nextLine();
            if ("exit".equals(dateString)) {
                flag = false;
                continue;
            }
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date parse = null;
            try {
                parse = simpleDateFormat.parse(dateString);
            } catch (ParseException e) {
                System.out.println(dateString);
                System.out.println("请输入 日期 日期格式是 yyyy-MM-dd HH:mm:ss");
            }
            assert parse != null;
            System.out.println(parse.getTime());

            LocalDateTime endTime = LocalDateTime.now();
            if (endTime.getMinute() - now.getMinute() == 1000) {
                System.exit(0);
            }
        }
        System.exit(0);
    }
}
