package cs223;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.text.*;
import java.lang.*;

import static java.lang.Math.abs;

//sample input:
//INSERT INTO thermometerobservation VALUES ('fe9e181d-10be-48fd-b579-5d7b0e93aa58', 68, '2017-11-08 00:00:00', '06ab568d_417e_4d3c_bb16_f167e2be065d');

public class Hasher
{
    public static int hash (String inputString) throws NoSuchAlgorithmException
    {
        //System.out.println(inputString);
        String sensorID = null;
        String timeStamp = null;

        String[] parameters = inputString.split(", ");
        String type = inputString.split(" ")[2];

        //System.out.println(type);

        if (type.equals("thermometerobservation"))
        {
            sensorID = (parameters[0].split("'"))[1];
            timeStamp = parameters[2].split("'")[1];
        }
        else if (type.equals("wemoobservation"))
        {
            sensorID = (parameters[0].split("'"))[1];
            timeStamp = parameters[3].split("'")[1];
        }
        else if (type.equals("wifiapobservation"))
        {
            sensorID = (parameters[0].split("'"))[1];
            timeStamp = parameters[2].split("'")[1];
        }

        int hash = 7;
        /*
        for (int i = 0; i < sensorID.length(); i++) {
            hash = hash*31 + sensorID.charAt(i);
        }

        for (int i = 0; i < timeStamp.length(); i++) {
            hash = hash*31 + timeStamp.charAt(i);
        }

         */

        MessageDigest md = MessageDigest.getInstance("MD5");
        String in = sensorID + timeStamp;
        md.update(in.getBytes());
        byte[] digest = md.digest();
        String out = DatatypeConverter.printHexBinary(digest).toUpperCase();

        //System.out.println(out);

        for (int i = 0; i < out.length(); i++) {
            hash = hash*31 + out.charAt(i);
        }

        //System.out.println(hash); 
        //System.out.println(sensorID); 
        //System.out.println(timeStamp); 

        return abs(hash) % Settings.NUM_COHORTS;
    }

    /*
    public static void main(String[] args) throws Exception
    {
        int x = hash(
                "INSERT INTO thermometerobservation VALUES ('fe9e181d-10be-48fd-b479-5d7b0e93aa58', 68, '2017-11-14 00:36:00', '06ab568d_417e_4d3c_bb16_f167e2be065d');"
        );
        System.out.println(x);
    }
     */
}