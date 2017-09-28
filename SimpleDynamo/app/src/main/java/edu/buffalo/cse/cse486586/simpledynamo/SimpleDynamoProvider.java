package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;

import static android.content.ContentValues.TAG;
import static android.content.Context.TELEPHONY_SERVICE;

public class SimpleDynamoProvider extends ContentProvider {


	public static final String PREFS_NAME = "MyPrefsFile";
	static final int SERVER_PORT = 10000;
	String myport="";


	String myhash="";
	String[] porttable={"11124","11112","11108","11116","11120"};
	String[] hashtable;
	int counter=0;
	String s1="";
	String s2="";

	boolean inserting = false;
	boolean recovering = false;
	int count =0;
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {

		SharedPreferences prefs = getContext().getSharedPreferences(PREFS_NAME, 0);
		if (selection.equals("@")) {

			prefs.edit().clear().apply();
		} else if (selection.equals("*")) {
			for (int i = 0; i < porttable.length; i++) {
				Socket socket = null;
				String remoteport = porttable[i];

				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remoteport));
				} catch (IOException e) {
					e.printStackTrace();
				}

				String msgToSend = "6$" + selection;
				OutputStream outToServer = null;


				try {
					outToServer = socket.getOutputStream();
				} catch (IOException e) {
					e.printStackTrace();
				}

				DataOutputStream out = new DataOutputStream(outToServer);


				try {
					out.writeUTF(msgToSend);
				} catch (IOException e) {
					e.printStackTrace();
				}
				try {
					Thread.sleep(30);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				try {
					out.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}


			}
		} else {
			String key = selection;
			String hashedkey = null;

			try {
				hashedkey = genHash(key);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

			String tempport = getport(hashedkey);
			if (tempport.equals(myport)) {
				prefs.edit().remove(selection).apply();
				String succ[]=getsucc(tempport);
				for(int i=0;i<2;i++)
				{
					Socket socket = null;
					String remoteport = succ[i];

					try {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(remoteport));
					} catch (IOException e) {
						e.printStackTrace();
					}


					String msgToSend = "7$" + selection;
					OutputStream outToServer = null;


					try {
						outToServer = socket.getOutputStream();
					} catch (IOException e) {
						e.printStackTrace();
					}


					DataOutputStream out = new DataOutputStream(outToServer);

					try {
						out.writeUTF(msgToSend);
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						Thread.sleep(30);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						out.flush();
					} catch (IOException e) {
						e.printStackTrace();
					}


				}


			} else {
				Socket socket = null;
				String remoteport = tempport;

				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remoteport));
				} catch (IOException e) {
					e.printStackTrace();
				}


				String msgToSend = "7$" + selection;
				OutputStream outToServer = null;


				try {
					outToServer = socket.getOutputStream();
				} catch (IOException e) {
					e.printStackTrace();
				}


				DataOutputStream out = new DataOutputStream(outToServer);

				try {
					out.writeUTF(msgToSend);
				} catch (IOException e) {
					e.printStackTrace();
				}
				try {
					Thread.sleep(30);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				try {
					out.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}


				String succ[]=getsucc(tempport);
				for(int i=0;i<2;i++)
				{

					remoteport = succ[i];

					try {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(remoteport));
					} catch (IOException e) {
						e.printStackTrace();
					}


					msgToSend = "7$" + selection;
					outToServer = null;


					if (socket != null) {
						try {
							outToServer = socket.getOutputStream();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}


					out = new DataOutputStream(outToServer);

					try {
						out.writeUTF(msgToSend);
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						Thread.sleep(30);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						out.flush();
					} catch (IOException e) {
						e.printStackTrace();
					}


				}
			}
		}

		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	//ipart
	@Override
	public Uri insert(Uri uri, ContentValues values) {

		Log.v("Insert request ",values.toString());


		if(recovering)
		{
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			Log.v("Waiting in","insert for recover");
		}
		inserting=true;

		//Log.v("Entered insert","insert");
		String key = values.getAsString("key");

		String hashedkey = null;
		try {
			hashedkey = genHash(key);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		String tempport = getport(hashedkey);
		//Log.v("Recog:",hashedkey+"  "+tempport);
		if(tempport.equals(myport))
		{
			SharedPreferences prefs = getContext().getSharedPreferences(PREFS_NAME, 0);
			SharedPreferences.Editor editor = prefs.edit();

			editor.putString(values.getAsString("key"), values.getAsString("value"));
			editor.apply();
			// TODO Auto-generated method stub
			Log.v("insert", values.toString());
			String[] succ = getsucc(myport);
			Socket socket = null;

			for (int i=0;i<2;i++) {
				String remoteport = succ[i];

				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remoteport));
				} catch (IOException e) {
					e.printStackTrace();
				}


				String msgToSend = "31$" + values.getAsString("key") + "$" + values.getAsString("value");

				OutputStream outToServer = null;


				try {
					outToServer = socket.getOutputStream();
				} catch (IOException e) {
					e.printStackTrace();
				}


				DataOutputStream out = new DataOutputStream(outToServer);

				try {
					out.writeUTF(msgToSend);
				} catch (IOException e) {
					e.printStackTrace();
				}
				try {
					Thread.sleep(30);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				try {
					out.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}



			}


			inserting=false;

			try {
				Thread.sleep(30);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			return uri;
		}
		else
		{
			String msg = "2$" + values.getAsString("key") + "$" + values.getAsString("value")+"$"+tempport;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myport);
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}



		}


		return null;
	}

	@Override
	public boolean onCreate() {
		Context context = this.getContext();
		TelephonyManager tel = (TelephonyManager) context.getSystemService(TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		myport=myPort;
		try {
			myhash=genHash(Integer.toString(Integer.parseInt(myport)/2));
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		String succ[]=getsucc(myPort);
		s1=succ[0];s2=succ[1];
		hashtable = gethashtable(porttable);
		SharedPreferences prefs = getContext().getSharedPreferences(PREFS_NAME, 0);
		Map<String,String> mymap = (Map<String, String>) prefs.getAll();
		if(mymap.size()!=0)
		{
			recovering=true;
			Log.v("Recovered:",myPort);
			try {

				prefs.edit().clear().apply();
				String msg = "1$" +myPort;
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myport);

			} catch (Exception e) {
				e.printStackTrace();
			}
			//prefs.edit().clear().apply();



		}


		try {

			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		}
		catch (Exception e)
		{

			Log.e(TAG, "Can't create a ServerSocket");

		}


		return false;
	}


	//stpart
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		public Uri buildUri(String scheme, String authority) {
			Uri.Builder uriBuilder = new Uri.Builder();
			uriBuilder.authority(authority);
			uriBuilder.scheme(scheme);
			return uriBuilder.build();
		}






		ContentValues keyValueToInsert = new ContentValues();

		@Override
		protected Void doInBackground(ServerSocket... sockets) {

			ServerSocket serverSocket = sockets[0];

			while (true) {


				Socket server = null;
				try {
					server = serverSocket.accept();
				} catch (IOException e) {
					e.printStackTrace();
				}

				DataInputStream in = null;
				try {
					in = new DataInputStream(server.getInputStream());
				} catch (IOException e) {
					e.printStackTrace();
				}
				String msg2 = null;
				try {
					msg2 = in.readUTF();
				} catch (IOException e) {
					e.printStackTrace();
				}
				String[] parts = msg2.split("\\$");
				int mode = Integer.parseInt(parts[0]);





				if(mode==3)
				{
					String key = parts[1];
					String value = parts[2];


					SharedPreferences prefs = getContext().getSharedPreferences(PREFS_NAME, 0);
					SharedPreferences.Editor editor = prefs.edit();

					editor.putString(key,value);
					editor.apply();
					OutputStream outToServer = null;
					try {
						outToServer = server.getOutputStream();
					} catch (IOException e) {
						e.printStackTrace();
					}
					DataOutputStream out = new DataOutputStream(outToServer);
					try {
						out.writeUTF("OK");
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						out.flush();
					} catch (IOException e) {
						e.printStackTrace();
					}

					// TODO Auto-generated method stub
					Log.v("insert", key+"  "+value);

					//For replicas:
					String[] succ = getsucc(myport);
					Socket socket = null;

					for (int i=0;i<2;i++) {
						String remoteport = succ[i];

						try {
							socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(remoteport));
						} catch (IOException e) {
							e.printStackTrace();
						}


						String msgToSend = "31$" + key + "$" + value;

						outToServer = null;


						try {
							outToServer = socket.getOutputStream();
						} catch (IOException e) {
							e.printStackTrace();
						}


						out = new DataOutputStream(outToServer);

						try {
							out.writeUTF(msgToSend);
						} catch (IOException e) {
							e.printStackTrace();
						}
						try {
							Thread.sleep(30);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						try {
							out.flush();
						} catch (IOException e) {
							e.printStackTrace();
						}


					}

				}

				else if (mode==31)
				{
					String key = parts[1];
					String value = parts[2];
					SharedPreferences prefs = getContext().getSharedPreferences(PREFS_NAME, 0);
					SharedPreferences.Editor editor = prefs.edit();

					editor.putString(key,value);
					editor.apply();

					// TODO Auto-generated method stub
					Log.v("replicate", key+"  "+value);



				}


				else if (mode==4)
				{

					String selection = parts[1];
					OutputStream outToServer = null;
					try {
						outToServer = server.getOutputStream();
					} catch (IOException e) {
						e.printStackTrace();
					}
					DataOutputStream out = new DataOutputStream(outToServer);
					SharedPreferences prefs = getContext().getSharedPreferences(PREFS_NAME, 0);
					String value = prefs.getString(selection, "");
					try {
						out.writeUTF(value);
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						out.flush();
					} catch (IOException e) {
						e.printStackTrace();
					}


				}
				else if (mode==5)
				{


					OutputStream outToServer = null;
					try {
						outToServer = server.getOutputStream();
					} catch (IOException e) {
						e.printStackTrace();
					}
					DataOutputStream out = new DataOutputStream(outToServer);
					SharedPreferences prefs = getContext().getSharedPreferences(PREFS_NAME, 0);
					Map star = prefs.getAll();
					//Log.v("Pref size",Integer.toString(star.size()));
					Set keys = star.keySet();
					String output = "";
					for (Iterator i = keys.iterator(); i.hasNext(); ) {
						String key = (String) i.next();
						String value = (String) star.get(key);
						//Log.v("P: ",key+" "+value);
						output = output + key + "$" + value + "$";
					}

					try {
						out.writeUTF(output);
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						out.flush();
					} catch (IOException e) {
						e.printStackTrace();
					}

				}

				else if(mode==6)
				{
					SharedPreferences prefs = getContext().getSharedPreferences(PREFS_NAME, 0);
					prefs.edit().clear().apply();
				}

				else if(mode==7)
				{
					String selection=parts[1];
					SharedPreferences prefs = getContext().getSharedPreferences(PREFS_NAME, 0);
					prefs.edit().remove(selection).apply();
				}

				//Log.v("Received:", msg2);

			}
		}

	}


	//ctpart
	private class ClientTask extends AsyncTask<String, Void, Void> {


		@Override
		protected Void doInBackground(String... msgs) {

			String[] parts = msgs[0].split("\\$");
			int mode = Integer.parseInt(parts[0]);



			if(mode==1)
			{

				recover(myport);
			}

			else if(mode==2)
			{
				String key=parts[1];
				String value = parts[2];
				Socket socket = null;
				String remoteport = parts[3];

				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remoteport));
				} catch (IOException e) {
					e.printStackTrace();
				}


				String msgToSend = "3$" + key+"$"+value;

				OutputStream outToServer = null;
				DataInputStream in = null;

				try {
					in = new DataInputStream(socket.getInputStream());
				} catch (IOException e) {
					e.printStackTrace();
				}


				if (socket != null) {
					try {
						outToServer = socket.getOutputStream();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}

				else
				{
					Log.v("Failure Found:","insert");
				}




				DataOutputStream out = new DataOutputStream(outToServer);
				try {
					out.writeUTF(msgToSend);
				} catch (IOException e) {
					e.printStackTrace();
				}
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				try {
					out.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}

				String ack = null;
				try {
					ack = in.readUTF();
				} catch (Exception e) {
					//e.printStackTrace();
					Log.v("Failure detected at ",remoteport);
					String[] succ = getsucc(remoteport);
					socket = null;

					for (int i=0;i<2;i++) {
						remoteport = succ[i];

						try {
							socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(remoteport));
						} catch (IOException e1) {
							e1.printStackTrace();
						}


						msgToSend = "31$" + key + "$" + value;

						outToServer = null;


						try {
							outToServer = socket.getOutputStream();
						} catch (IOException e2) {
							e2.printStackTrace();
						}


						out = new DataOutputStream(outToServer);

						try {
							out.writeUTF(msgToSend);
						} catch (IOException e3) {
							e3.printStackTrace();
						}
						try {
							Thread.sleep(200);
						} catch (InterruptedException e4) {
							e4.printStackTrace();
						}
						try {
							out.flush();
						} catch (IOException e5) {
							e5.printStackTrace();
						}


					}
				}

//				if (ack!=null)
//				{Log.v("ACK",ack);}
			inserting=false;


			}


			return null;
		}

		//rpart
		private void recover(String port)
		{


			String succ[]=getsucc(port);
			String s=succ[0];
			String pred[]=getpred(port);
			String p1=pred[0];
			String p2=pred[1];
			String[] rports={s,p1,p2};
			String[] tempoutput=new String[3];

			for (int i=0;i<rports.length;i++)
			{
				Socket socket = null;
				String remoteport = rports[i];

				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remoteport));
				} catch (Exception e) {
					Log.v("Weird Exception at:",remoteport);
					e.printStackTrace();
				}


				String msgToSend = "5$" +"*";
				OutputStream outToServer = null;


				try {
					outToServer = socket.getOutputStream();
				} catch (IOException e) {
					e.printStackTrace();
				}


				DataOutputStream out = new DataOutputStream(outToServer);

				try {
					out.writeUTF(msgToSend);
				} catch (IOException e) {
					e.printStackTrace();
				}
				try {
					Thread.sleep(30);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				try {
					out.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}


				DataInputStream in = null;

				try {
					in = new DataInputStream(socket.getInputStream());
				} catch (IOException e) {
					e.printStackTrace();
				}


				String op = null;
				try {
					op = in.readUTF();
				} catch (Exception e) {
					e.printStackTrace();
				}
				tempoutput[i]=op;


			}

			Map<String,String> smap=getstar(tempoutput[0]);
			Map<String,String> p1map=getstar(tempoutput[1]);
			Map<String,String> p2map=getstar(tempoutput[2]);

			SharedPreferences prefs = getContext().getSharedPreferences(PREFS_NAME, 0);
			SharedPreferences.Editor editor = prefs.edit();

			Set keys = smap.keySet();
			for (Iterator i = keys.iterator(); i.hasNext(); ) {
				String key = (String) i.next();
				String value = smap.get(key);
				String hashedkey=null;
				try {
					hashedkey = genHash(key);
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}

				String tport = getport(hashedkey);
				if(tport.equals(port)) {
					editor.putString(key, value);
					editor.apply();
				}


			}

			keys = p1map.keySet();
			for (Iterator i = keys.iterator(); i.hasNext(); ) {
				String key = (String) i.next();
				String value = p1map.get(key);
				String hashedkey=null;
				try {
					hashedkey = genHash(key);
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}

				String tport = getport(hashedkey);
				if(tport.equals(p1)) {
					editor.putString(key, value);
					editor.apply();
				}


			}

			keys = p2map.keySet();
			for (Iterator i = keys.iterator(); i.hasNext(); ) {
				String key = (String) i.next();
				String value = p2map.get(key);
				String hashedkey=null;
				try {
					hashedkey = genHash(key);
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}

				String tport = getport(hashedkey);
				if(tport.equals(p2)) {
					editor.putString(key, value);
					editor.apply();
				}
			}
			recovering=false;

		}

	}





	//qpart
	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder)
	{
		Log.v("Query request:",selection);


		try {
			Thread.sleep(400);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		while(inserting)
		{

			Log.v("Waiting in","query for insert");
		}

		while(recovering)
		{

			Log.v("Waiting in","query for recover");
		}







		//Log.v("Entered query","query");

		SharedPreferences prefs = getContext().getSharedPreferences(PREFS_NAME, 0);

		if (selection.equals("@")) {
			Map localmap = prefs.getAll();
			String cnames[] = {"key", "value"};
			MatrixCursor matrixCursor = new MatrixCursor(cnames, 2);
			Set keys = localmap.keySet();
			for (Iterator i = keys.iterator(); i.hasNext(); ) {
				String key = (String) i.next();
				String value = (String) localmap.get(key);
				String keyvalue[] = {key, value};
				matrixCursor.addRow(keyvalue);

			}
			return matrixCursor;

		}
		else if(selection.equals("*"))
		{
			String Output ="";
			for (int i=0;i<porttable.length;i++)
			{
				Socket socket = null;
				String remoteport = porttable[i];

				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remoteport));
				} catch (IOException e) {
					e.printStackTrace();
				}


				String msgToSend = "5$" +selection;
				OutputStream outToServer = null;


				try {
					outToServer = socket.getOutputStream();
				} catch (IOException e) {
					e.printStackTrace();
				}


				DataOutputStream out = new DataOutputStream(outToServer);

				try {
					out.writeUTF(msgToSend);
				} catch (IOException e) {
					e.printStackTrace();
				}
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				try {
					out.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}


				DataInputStream in = null;

				try {
					in = new DataInputStream(socket.getInputStream());
				} catch (IOException e) {
					e.printStackTrace();
				}


				String output = null;
				try {
					output = in.readUTF();
					Output=Output+output;
				} catch (IOException e) {
					e.printStackTrace();
				}



			}
			Map<String,String> star = getstar(Output);
			String cnames[] = {"key", "value"};
			MatrixCursor matrixCursor = new MatrixCursor(cnames, 2);
			Set keys = star.keySet();
			for (Iterator i = keys.iterator(); i.hasNext(); ) {
				String key = (String) i.next();
				String value = (String) star.get(key);
				String keyvalue[] = {key, value};
				matrixCursor.addRow(keyvalue);

			}
			//Log.v("Number of rows",Integer.toString(star.size()));
			return matrixCursor;

		}
		else {
			String key= selection;
			String hashedkey = null;

			try {
				hashedkey = genHash(key);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

			String tempport = getport(hashedkey);
			if(tempport.equals(myport))
			{
				String value = prefs.getString(selection, "");
				String cnames[] = {"key", "value"};
				MatrixCursor matrixCursor = new MatrixCursor(cnames, 2);
				String keyvalue[] = {selection, value};
				matrixCursor.addRow(keyvalue);

				return matrixCursor;
			}
			else
			{
				Socket socket = null;
				String succ[]=getsucc(tempport);

				String remoteport = tempport;
				Log.v("Successor port",succ[1]);


				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remoteport));
				} catch (Exception e) {
					e.printStackTrace();
				}


				String msgToSend = "4$" +selection;
				OutputStream outToServer = null;

				try {
					outToServer = socket.getOutputStream();
				} catch (Exception e2) {
					Log.v("Query at "+succ[1],"failed 1");
					e2.printStackTrace();
				}
				try {
					DataOutputStream out = new DataOutputStream(outToServer);

					out.writeUTF(msgToSend);
					Thread.sleep(100);
					out.flush();
				} catch (Exception e3) {
					Log.v("Query at "+succ[1],"failed 2");
					e3.printStackTrace();
				}


				DataInputStream in = null;
				try {
					in = new DataInputStream(socket.getInputStream());
				} catch (Exception e) {
					Log.v("Query at "+succ[1],"failed 3");
					e.printStackTrace();
				}
				String output = null;
				try {
					output = in.readUTF();
					if(output==null||output.equals(""))
					{
						Log.v("Query at "+succ[1],"failed 4");
						//e.printStackTrace();
						output = querybackup(succ[0],selection,succ[1]);
					}
					Log.v("Retrieved "+output,"from "+succ[1]);
				} catch (Exception e) {


					Log.v("Query at "+succ[1],"failed 4");
					//e.printStackTrace();
					output = querybackup(succ[0],selection,tempport);
				}
				counter=counter+1;
				//Log.v("output received:",output+" "+Integer.toString(counter));
				String cnames[] = {"key", "value"};
				MatrixCursor matrixCursor = new MatrixCursor(cnames, 2);
				String keyvalue[] = {selection, output};
				matrixCursor.addRow(keyvalue);
				return matrixCursor;
			}

		}



	}


	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
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


	private String[] gethashtable(String[] a)
	{
		String output[]=new String[a.length];
		for(int i=0;i<a.length;i++)
		{

			try {
				output[i]=genHash(Integer.toString(Integer.parseInt(a[i]) / 2));
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}


		}

		return output;
	}

	private String getport(String hkey)
	{
		String tempport="";
		boolean found = false;

		for (int i=0;i<porttable.length;i++)
		{

			if(hashtable[i].compareTo(hkey)>0)
			{

				tempport=porttable[i];
				found = true;
				break;
			}
		}

		if(found==false)
		{
			tempport=porttable[0];
		}


		return tempport;
	}


	private Map getstar(String s)
	{
		Map<String,String> star=new HashMap<String, String>();

		String[] parts = s.split("\\$");
		//Log.v("Parts: ",Integer.toString(parts.length));
		int n = parts.length/2;
		//Log.v("n: ",Integer.toString(n));
		for(int i=0;i<parts.length;i++)
		{
			//Log.v("S: ",parts[i]+" "+parts[i+1]);
			star.put(parts[i],parts[i+1]);
			i++;
		}

		return star;
	}

	private String[] getsucc(String port)
	{
		String succ[]=new String[2];
		if (port.equals("11124"))
		{succ[0]="11112";succ[1]="11108";}
		if (port.equals("11112"))
		{succ[0]="11108";succ[1]="11116";}
		if (port.equals("11108"))
		{succ[0]="11116";succ[1]="11120";}
		if (port.equals("11116"))
		{succ[0]="11120";succ[1]="11124";}
		if (port.equals("11120"))
		{succ[0]="11124";succ[1]="11112";}
		return succ;

	}

	private String[] getpred(String port)
	{
		String pred[]=new String[2];
		if (port.equals("11124"))
		{pred[0]="11120";pred[1]="11116";}
		if (port.equals("11112"))
		{pred[0]="11124";pred[1]="11120";}
		if (port.equals("11108"))
		{pred[0]="11112";pred[1]="11124";}
		if (port.equals("11116"))
		{pred[0]="11108";pred[1]="11112";}
		if (port.equals("11120"))
		{pred[0]="11116";pred[1]="11108";}
		return pred;

	}

	private String querybackup(String remoteport,String selection,String ogport)
	{
		Socket socket = null;
		try {
			socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(remoteport));
		} catch (Exception e) {
			e.printStackTrace();
		}


		String msgToSend = "4$" +selection;
		OutputStream outToServer = null;

		try {
			outToServer = socket.getOutputStream();
		} catch (Exception e2) {
			Log.v("Query at "+remoteport,"failed 1");
			e2.printStackTrace();
		}
		try {
			DataOutputStream out = new DataOutputStream(outToServer);

			out.writeUTF(msgToSend);
			//Thread.sleep(100);
			out.flush();
		} catch (Exception e3) {
			Log.v("Query at "+remoteport,"failed 2");
			e3.printStackTrace();
		}


		DataInputStream in = null;
		try {
			in = new DataInputStream(socket.getInputStream());
		} catch (Exception e) {
			Log.v("Query at "+remoteport,"failed 3");
			e.printStackTrace();
		}
		String output = null;
		try {
			output = in.readUTF();
			if(output==null||output.equals(""))
			{
				Log.v("Query at "+remoteport,"failed 4");
				//e.printStackTrace();
				output = querybackup(ogport,selection,ogport);
			}
			Log.v("Retrieved "+output,"from "+remoteport);
		} catch (Exception e) {


			//e.printStackTrace();
			Log.v("Query at "+remoteport,"failed 4");
			output = querybackup(ogport,selection,ogport);

		}

		return output;
	}






}
