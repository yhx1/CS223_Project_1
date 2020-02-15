package cs223;

import java.io.*;
import java.util.*;
import java.text.*;




class QueryParser
{
    static class Composite
    {
        Long time;
        String query;
    }

    static ArrayList<Composite> parseTime(String fileName) throws Exception
    {
        ArrayList<Composite> times = new ArrayList<Composite>();
        FileReader input = null;
        input = new FileReader(fileName);
        BufferedReader bufRead = new BufferedReader(input);
        String myLine = null;
        String query = null;
        while ( (myLine = bufRead.readLine()) != null)
        {
            if (myLine.charAt(0) == '2')
            {
                String timestamp = (myLine.split(","))[0];
                String[] timeSplit = timestamp.split("T");
                timeSplit[1] = timeSplit[1].substring(0, 8);
                String newTime = timeSplit[0]+" "+timeSplit[1];

                DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date reference0 = dateFormat.parse("2017-11-08 01:23:00");

                Date time = dateFormat.parse(newTime);
                Long seconds = (time.getTime() - reference0.getTime());
                //System.out.println(query); 

                Composite current = new Composite();
                current.query = query;
                current.time = seconds;
                times.add(current);
                query = "";
            }
            else
            {
                query+=myLine;
                query+=" ";
            }

        }
/*
        for (Composite each : times) { 		      
            System.out.println(each.time.toString()+ each.query); 		
       }
*/
        return times;
    }

    public static void main(String[] args) throws Exception
    {
        parseTime(args[0]);
    }
}