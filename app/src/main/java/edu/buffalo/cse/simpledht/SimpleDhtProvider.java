package edu.buffalo.cse.cse486586.simpledht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;

public class SimpleDhtProvider extends ContentProvider {


    String myHashId;
    public String myPort;
    public int nodeCount;
    private HashMap<String,Integer> queryHashMap;
    public String portToGod;
    public String mySuccessor,myPredessor,mySuccessorPort,myPredessorPort;
    Object lock;
    int starDelete;
    private Uri mUri;
    private int queryFlag;
    private SQLiteDatabase db;
    private Forsql forsql;
    public ContentValues cv;
    public Cursor queryCursor;
    public Cursor globalDumpCursor;
    public int starQueryFlag;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        String deleteHashValue=null;
        try{
            deleteHashValue=genHash(selection);}catch(NoSuchAlgorithmException e){}


        if(selection.equals("@"))
        {
            int ret= db.delete("mytable",null,null);
            if(ret<0)
                Log.v("Delete@","LocalDumpFail");
            else
                Log.v("Delete@","LocalDumpSuccess");
        }
        else if(selection.equals("*"))
        {
            int ret= db.delete("mytable",null,null);
            if(ret<0)
                Log.v("Delete*","LocalDumpFail");
            else
                Log.v("Delete*","LocalDumpSuccess");

            if(mySuccessorPort!=null){
            String sendDeleteMessage="9"+"~"+selection+"~"+myPort;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sendDeleteMessage, mySuccessorPort);
            }

            starDelete=0;
            while(starDelete==0)
            {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        else if(mySuccessorPort==null)
        {
            //I have the data
            String []array={selection};
            int ret= db.delete("mytable","key=?",array);
            if(ret<0)
                Log.v("Delete","Fail-"+selection);
            else
                Log.v("Delete","Success-"+selection);

        }
        else if(lookUp(deleteHashValue)==true )
        {
            //I have the data
            String []array={selection};
            int ret= db.delete("mytable","key=?",array);
            if(ret<0)
                Log.v("Delete","Fail-"+selection);
            else
                Log.v("Delete","Success-"+selection);

        }
        else
        {
            //Passto successor
            String sendDeleteMessage="8"+"~"+selection+"~"+deleteHashValue;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sendDeleteMessage, mySuccessorPort);
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        String key = (String)values.get(forsql.col1);
        String value=(String)values.get(forsql.col2);
        String insertHashKey=null;
        try{
            insertHashKey=genHash(key);
        }catch(NoSuchAlgorithmException e){}

        if(mySuccessorPort==null)
        {
            //I am the only node
            cv=new ContentValues();
            cv.put("key",key);
            cv.put("value",value);
            long ret=db.insertWithOnConflict("mytable", null, cv,SQLiteDatabase.CONFLICT_REPLACE);
            if(ret<0)
                Log.v("IntertFail","SupermanIsFalseGod");
            else
                Log.v("Insert","AllHailMegatron");

        }
        else if(lookUp(insertHashKey)==true)
        {
            //I have to insert it
            cv=new ContentValues();
            cv.put("key",key);
            cv.put("value",value);
            long ret=db.insertWithOnConflict("mytable", null, cv,SQLiteDatabase.CONFLICT_REPLACE);
            if(ret<0)
                Log.v("IntertFail","FalseGod");
            else
                Log.v("Insert","AllHailMegatron");

        }
        else{
            Log.v("Insert","Hey "+mySuccessorPort+" insert "+key+"");
            String sendInsertMessage="3"+"~"+key+"~"+value+"~"+insertHashKey;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sendInsertMessage, mySuccessorPort);
        }



        return null;
    }

    @Override
    public boolean onCreate() {

        // TODO Auto-generated method stub
        nodeCount=0;
        mySuccessor="IHaveNoChildren";
        myPredessor=null;
        mySuccessorPort=null;
        myPredessorPort="a";
        queryHashMap=new HashMap<String,Integer>();
        portToGod="11108";
        starQueryFlag=0;
        lock=new Object();
        starDelete=0;
        forsql=new Forsql(getContext());
        db=forsql.getWritableDatabase();
        Log.v("CreateNewInstance","--------------------------");

        if(db==null)
            return false;
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");




       //Step 1 - Get myPort
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {
            ServerSocket serverSocket = new ServerSocket(10000);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {

            e.printStackTrace();
            //return;
        }


        if(myPort.equals(portToGod))
        {
            //I am GOD
            //My Hashed ID
            Log.v("Hello","I am god");
            try{
                myHashId=genHash("5554");
            }catch(NoSuchAlgorithmException e){}
        }
        else
        {
            //Step 3 - Join message to God
            String id="d";
            if(myPort.equals("11112"))
            {
                id="5556";
            }
            else if(myPort.equals("11116"))
            {
                id="5558";
            }
            else if(myPort.equals("11120"))
            {
                id="5560";
            }
            else if(myPort.equals("11124"))
            {
                id="5562";
            }
            try{
                myHashId=genHash(id);
            }catch(NoSuchAlgorithmException e){}
            String joinMessage= "1"+"~"+myHashId+"~"+myPort;
            Log.v("MyPortNo",""+myPort);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, joinMessage, portToGod);

        }





        return true;
    }

    @Override
     public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        SQLiteQueryBuilder forcursor=new SQLiteQueryBuilder();
        forcursor.setTables("mytable");
        String queryHashValue=null;
        try{
        queryHashValue=genHash(selection);}catch(NoSuchAlgorithmException e){}
        String[] array= {selection};
        String tname="mytable";
        String key="key";
        String allQuery="SELECT * FROM "+tname+" ;";
        Log.v("Query Start", ""+selection);

        if(selection.equals("@"))
        {
            Log.v("Query","@,*");
            Cursor c=db.rawQuery(allQuery, null);
            return c;
        }
        else if(mySuccessorPort==null)
        {
            if(selection.equals("*"))
            {
                Log.v("Query","GiveMeAllYourMoney");

                Cursor c=db.rawQuery(allQuery, null);
                return c;
            }
            else
            {
            Cursor c=db.query(true,tname,null,"key = ? ",array,null,null,null,null);
            return c;
            }
        }
        else if(selection.equals("*"))
        {
            //Global Dump
            Log.v("StarQuery","Begin-"+myPort);
            String globalQueryMessage="6"+"~"+myPort+"~"+"dummy"+"~";
            Cursor c=db.rawQuery(allQuery, null);
            c.moveToFirst();



          if(c.getCount()>0)
          {
              do {

                  int keyIndex= c.getColumnIndex("key");
                  int valueIndex=c.getColumnIndex("value");
                  String keyValue=c.getString(keyIndex);
                  String valueValue=c.getString(valueIndex);
                  globalQueryMessage=globalQueryMessage+"#"+keyValue+"#"+valueValue;
              }while (c.moveToNext());
          }
            c.close();
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, globalQueryMessage, mySuccessorPort);

            starQueryFlag=0;
            while(starQueryFlag==0)
            {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            Log.v("StarQuery","LordMegatronRisesAgain");
            return globalDumpCursor;
        }
        else if(lookUp(queryHashValue)==true)
        {
            Log.v("*Query","I have it "+key);
            Cursor c=db.query(true, tname, null, "key = ? ", array, null, null, null, null);

            return c;
        }
        else
        {
            Log.v("Query","Hey "+mySuccessorPort+" query this "+selection);
            String sendQueryMessage="4"+"~"+selection+"~"+queryHashValue+"~"+myPort;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sendQueryMessage, mySuccessorPort);
            queryHashMap.put(selection,0);
           /* while(queryHashMap.get(selection)==0)
            {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }*/
            while(queryHashMap.get(selection)==0) {
                try {
                    Thread.sleep(1500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            Log.v("@Query","LordMegatrinRisesAgain");
            return queryCursor;

        }

        //return c;
        //Cursor c = db.rawQuery("select key, value from " + tname +" where key = "+selection+";",array );
        //return forcursor.query(db,null," key= ? ",array,null,null,null);

       // Log.v("query",""+c.getString(c.getColumnIndex("key")));


    }
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private boolean lookUp(String nodeId) {

        if (myPredessor.compareTo(myHashId) > 0)
        {

            if (myHashId.compareTo(nodeId) >= 0 || myPredessor.compareTo(nodeId) < 0)
            {
                return true;
            }
            else {


                return false;
            }
        }

        if(myHashId.compareTo(nodeId)>=0 && myPredessor.compareTo(nodeId)<0)
        {

            return true;
        }

        return false;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            DataInputStream in;
            Socket sock;
            InputStream is;

            while(true){
                try {
                    sock = serverSocket.accept();
                    is=sock.getInputStream();
                    in =  new DataInputStream(is);
                    String recdMess=in.readUTF();
                    String[] value_split = recdMess.split("~");
                    publishProgress(value_split);


                }catch(Exception e){}}

        }

        protected void onProgressUpdate(String...strings) {
            //Type~nodeHashId~nodePortNo
            //synchronized (lock)
            {
                int type = Integer.parseInt(strings[0].trim());
                String nodeHashId = strings[1].trim();
                String nodePortNo = strings[2].trim();

                if (type == 1) {
                    if (mySuccessor.equals("IHaveNoChildren")) {
                        //This is the first node join
                        mySuccessor = nodeHashId;
                        mySuccessorPort = nodePortNo;
                        myPredessorPort=nodePortNo;
                        myPredessor = nodeHashId;
                        nodeCount=1;
                        String assignSuccessorMessage = "2" + "~" + myHashId + "~" + myPort + "~" + myHashId+"~"+myPort;
                        Log.v("MyFirstBorn", "" + mySuccessorPort);
                        Log.v("MyFirstParent", "" + myPredessorPort);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, assignSuccessorMessage, nodePortNo);

                    } else {
                        //This is not the first join
                        if (lookUp(nodeHashId) == true) {
                            String assignSuccessorMessage = "2" + "~" + myHashId + "~" + myPort + "~" + myPredessor + "~" + myPredessorPort;
                            myPredessor = nodeHashId;
                            myPredessorPort = nodePortNo;
                         //   Log.v("Data ", "------------------------------------------");
                           // Log.v("Successor", mySuccessorPort);
                            //Log.v("Predessor", myPredessorPort);
                            nodeCount++;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, assignSuccessorMessage, nodePortNo);
                        } else {
                            String sendJoinMessage = "1"+"~"+nodeHashId+"~"+nodePortNo+"~"+mySuccessorPort;
                            //Log.v("MovingOn", "Hey,"+mySuccessorPort+" check if"+nodePortNo+" is your successor");
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sendJoinMessage, mySuccessorPort);
                        }
                    }
                } else if(type==2){
                    //To change the successor for new node joins
                    mySuccessor = nodeHashId;
                    mySuccessorPort = nodePortNo;
                    myPredessor = strings[3].trim();
                    myPredessorPort=strings[4].trim();
                   // Log.v("Data", "------------------------------------------");
                   // Log.v("Successor", mySuccessorPort);
                   // Log.v("Predessor", myPredessorPort);

                    nodeCount++;
                    String setPredessorMessage="7"+"~"+myHashId+"~"+myPort;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, setPredessorMessage, myPredessorPort);
                    //TODO : Check if we need to add predessor, if so send a message to my successor to add me a s predessor

                }
                else if(type==3)
                {
                    //Insert Message
                    String hashKey=strings[3].trim();
                    if(lookUp(hashKey)==true)
                    {
                        //I must insert it
                        Log.v("Insert","Ive got this one "+nodeHashId);
                        cv=new ContentValues();
                        cv.put("key",nodeHashId);
                        cv.put("value",nodePortNo);
                        insert(mUri,cv);
                    }
                    else
                    {
                        //pass it on
                       Log.v("Insert","Hey "+mySuccessorPort+" insert "+nodeHashId);
                        String passInsertMessage="3"+"~"+nodeHashId+"~"+nodePortNo+"~"+strings[3].trim();
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, passInsertMessage, mySuccessorPort);
                    }
                }
                else if(type==4)
                {
                    //Query Message

                    String hashQuery=strings[2].trim();
                    String queryterm=strings[1].trim();
                    Log.v("Type4","QueryMessage-"+queryterm);
                    if(lookUp(hashQuery)==true)
                    {



                        String[] array= {queryterm};
                        String tname="mytable";
                        Cursor c=db.query(true,tname,null,"key = ? ",array,null,null,null,null);
                        Log.v("QueryMan", "before get string");
                        if(c.moveToFirst())
                        {
                            int col1=c.getColumnIndex("key");
                            int col2=c.getColumnIndex("value");

                            String keyValue=c.getString(col1);
                            String valueValue=c.getString(col2);

                            Log.v("QuerySuccess","I Have it "+keyValue);

                            String queryMessage= "5"+"~"+keyValue+"~"+valueValue;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryMessage, strings[3].trim());
                        }
                        else
                            Log.v("QueryFail","For Some Reason");
                    }
                    else
                    {
                        Log.v("QueryPassOn","Hey "+mySuccessorPort+" check query "+ queryterm);
                        String sendQueryMessage="4"+"~"+queryterm+"~"+hashQuery+"~"+strings[3].trim();
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sendQueryMessage, mySuccessorPort);
                    }
                }
                else if(type==5)
                {
                    //Query return
                    String thevalue=strings[2].trim();
                    String thekey=strings[1].trim();
                    Log.v("Query-Origin",myPort+" got it it");
                    MatrixCursor mc=new MatrixCursor(new String[] {"key","value"} );
                    mc.addRow(new Object[] {thekey,thevalue});
                    mc.moveToFirst();
                    queryCursor=mc;
                    queryHashMap.put(nodeHashId,1);
                }
                else if(type==6)
                {
                    // Global Dump Query
                    //TODO : fix # count and move tofirst beforeiterating
                    if(nodeHashId.equals(myPort)) {
                        Log.v("StarQuery", "BacktoSource-" + myPort);
                        MatrixCursor globalCursor = new MatrixCursor(new String[]{"key", "value"});
                        if (strings.length > 3)
                        {

                            String allData = strings[3].trim();
                            allData = allData.substring(1);
                            String[] allDataSplit = allData.split("#");

                            if (allDataSplit.length > 1)
                            {
                                for (int i = 0; i < allDataSplit.length; i += 2)
                                {
                                    globalCursor.addRow(new Object[]{allDataSplit[i], allDataSplit[i + 1]});
                                }
                                globalCursor.moveToFirst();
                            }
                        }

                        globalDumpCursor=globalCursor;
                        starQueryFlag=1;

                    }
                    else
                    {
                        String tname="mytable";
                        String allQuery="SELECT * FROM "+tname+" ;";
                        Log.v("StarQuery","TakeMyMoney-"+myPort);

                        Cursor c=db.rawQuery(allQuery, null);
                        c.moveToFirst();

                        String queryMessage=null;

                        if(strings.length>3)
                        {
                            queryMessage="6"+"~"+nodeHashId+"~"+nodePortNo+"~"+ strings[3].trim();
                        }
                        else
                        {
                            queryMessage="6"+"~"+nodeHashId+"~"+nodePortNo+"~";
                        }

                       if(c.getCount()>0)
                       {
                           do{
                               int col1=c.getColumnIndex("key");
                               int col2=c.getColumnIndex("value");
                               String keyValue=c.getString(col1);
                               String valueValue=c.getString(col2);

                               queryMessage=queryMessage+"#"+keyValue+"#"+valueValue;

                           }while(c.moveToNext());
                       }
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryMessage, mySuccessorPort);
                    }

                }
                else if(type==7)
                {
                    //set Successor
                    mySuccessor=nodeHashId;
                    mySuccessorPort=nodePortNo;
                    Log.v("Data", "------------------------------------------");
                    Log.v("Successor", mySuccessorPort);
                    Log.v("Predessor", myPredessorPort);

                }
                else if(type==8)
                {
                   if(lookUp(nodePortNo)==true)
                   {
                       //I have it
                       delete(mUri,nodeHashId,null);
                   }
                   else
                   {
                    //Pass on to successor
                       String sendDeleteMessage="8"+"~"+nodeHashId+"~"+nodePortNo;
                       new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sendDeleteMessage, mySuccessorPort);
                   }

                }
                else if(type==9)
                {
                    String origin=nodePortNo;
                    if(origin.equals(myPort))
                    {
                        starDelete=1;
                    }
                    else
                    {
                        delete(mUri, "@", null);
                        String sendDeleteMessage="9"+"~"+nodeHashId+"~"+origin;
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sendDeleteMessage, mySuccessorPort);
                    }
                }

                return;
            }
        }//sync
    }
    //|||||||||||||||||||||||||||||||||||||||||||||||||SERVER TASK ENDS|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    //|||||||||||||||||||||||||||||||||||||||||||||||||Client TASK|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||


    private class ClientTask extends AsyncTask<String,Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {

                Socket socket;
                String portToSend=msgs[1].trim();

                socket=new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(portToSend));
                String msgToSend = msgs[0];
                Log.v("clientFunc",""+msgToSend);
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF(msgToSend);
                socket.close();


            } catch (UnknownHostException e) {
                Log.e("ff", "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e("ff", "ClientTask socket IOException");
            }

            return null;
        }
    }
    //|||||||||||||||||||||||||||||||||||||||||||||||||Client TASK ENDS|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||


}
