package com.example;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


public class testing {
public static void main(String[] args) {
	String t = "6/14/1999";
	String startDateString = "2013-03-26";
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd"); 
    Date startDate=null;
    String newDateString = null;
    try 
    {
        startDate = df.parse(startDateString);
        newDateString = df.format(startDate);
        Date td = new Date();
        @SuppressWarnings("deprecation")
		Date d = new Date(t);
        System.out.println( td.getYear() - d.getYear() );
    } catch (Exception e) 
    {
        e.printStackTrace();
    }} 
}
