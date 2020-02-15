package cs223;

import java.io.*;
import java.util.*;
import java.text.*;
import java.lang.*;


class QueryParser
{
    static TreeMap<Integer, HashMap<String, ArrayList<String>>> parseTime(String fileName) throws Exception
    {
        TreeMap<Integer, HashMap<String, ArrayList<String>>> queryStatementsWithTime = new TreeMap<Integer, HashMap<String, ArrayList<String>>>();

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
                Date reference0 = dateFormat.parse("2017-11-08 00:00:00");

                Date time = dateFormat.parse(newTime);
                Integer seconds = (int)((time.getTime() - reference0.getTime())/(1440*1000));

                //System.out.println(query);
                if (query != null)
                {
                    query = query.trim();
                    if (queryStatementsWithTime.containsKey(seconds))
                    {
                        queryStatementsWithTime.get(seconds).get("Query").add(query+";");
                    }
                    else
                    {
                        HashMap<String, ArrayList<String>> queryStatements = new HashMap<String, ArrayList<String>>();
                        ArrayList<String> newList = new ArrayList<String>();
                        newList.add(query+";");
                        queryStatements.put("Query", newList);
                        queryStatementsWithTime.put(seconds, queryStatements);
                    }
                }
                query = "";
            }
            else
            {
                if (myLine.equals("\"") == false)
                {
                    query+=myLine.trim();
                    query+=" ";
                }
            }

        }

        return queryStatementsWithTime;
    }

    public static void main(String[] args) throws Exception
    {
        /*
        TreeMap<Integer, HashMap<String, ArrayList<String>>> test = parseTime("Resources/queries/high_concurrency/queries.txt");
        Iterator testIterator = test.entrySet().iterator();

        // Iterate through the hashmap
        while (testIterator.hasNext()) {
            Map.Entry mapElement = (Map.Entry)testIterator.next();
            System.out.println(mapElement.getKey());
            System.out.println(mapElement.getValue());
        }
        */
    }
}