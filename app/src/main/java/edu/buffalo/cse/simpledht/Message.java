package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;

/**
 * Created by arjunsun on 3/24/16.
 */
public class Message implements Serializable {

    public static String mess_text;     //To store the message
    public static int type;            //To keep count of replies
    public static String myHashId;             // Type [1=Asking,2=Propose,3=isDeliverable]
    public static String myPort;        // Unique identifier [SenderPortNo+SenderIndex]




}
