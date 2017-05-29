package org.apache.flink.table.api.java;

import org.apache.calcite.avatica.util.DateTimeUtils;

import java.sql.Date;
/**
 * Created by jincheng.sunjc on 17/5/28.
 */
public class JTest {
public static void main(String[] args) throws Exception{
    Date date =  Date.valueOf("2017-05-28");
    int result = org.apache.calcite.runtime.SqlFunctions.toInt(date) ;
    java.sql.Date resultDate = org.apache.calcite.runtime.SqlFunctions.internalToDate(result);
    System.out.println(result);
    System.out.println(date.getTime() / DateTimeUtils.MILLIS_PER_DAY);
    System.out.println(resultDate);
}
}
