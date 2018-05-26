package azkaban.test;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TimeTest {
    public static void main(String[] args) {
        String timeFormat = "yyyyMM";
        int offset = -1;
        //todo
        //1.判断时间格式正确
        //2.生成当时的数据加上偏移量来作为数据时间传给任务
        SimpleDateFormat formatt = new SimpleDateFormat(timeFormat);
        Calendar rightNow = Calendar.getInstance();
        Date dt1=rightNow.getTime();
        switch (timeFormat.length()){
            case 6:
                rightNow.add(Calendar.MONTH,offset);
                break;
            case 8:
                rightNow.add(Calendar.DAY_OF_YEAR,offset);
                break;
            case 10:
                rightNow.add(Calendar.HOUR_OF_DAY,offset);
                break;
            default:
                rightNow.add(Calendar.DAY_OF_YEAR,offset);
        }
        String dataTime = formatt.format(rightNow.getTime());
        System.out.println(dataTime);
    }
}