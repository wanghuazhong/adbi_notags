package util;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.sohu.ADRD.StructuralLabelBase.service.LabelBaseDumpService;
import com.sohu.ADRD.StructuralLabelBase.type.Tag;

public class UserTagsDumper {
	
	private static final int TIMEOUT = 30 * 1000;
	private static final int SLEEP = 30 * 1000;
	private static final String TAG_IP = "192.168.230.103";
	private static final int TAG_PORT = 7923;
	
	public Map<Long,Tag> DumpLong(List<Short> typelist) {
		
		int retry = 0;
		while(true) {
			TSocket socket = new TSocket(TAG_IP, TAG_PORT);
			socket.setTimeout(TIMEOUT);
			TTransport transport = socket;
			try {
				TProtocol protocol = new TCompactProtocol(transport);
				LabelBaseDumpService.Client client = new LabelBaseDumpService.Client(protocol);
				transport.open();
				Map<Long, Tag> dumptag = (HashMap<Long, Tag>)client.dump_int(typelist);
				return dumptag;
			} catch(Exception ex) {
				System.err.print("retry = " + retry + "\n" + ex.toString());
				try {
					Thread.sleep(SLEEP);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} finally {
				retry ++;
				transport.close();
			}
		}
	}
	
	
	/**
	 * dump��ǩ��Map<String, Long>
	 * @param typelist
	 * @return
	 * @throws TException
	 */
	public Map<String, Long> DumpString_Light(List<Short> typelist)  throws TException{
		
		int retry = 0;
		while(true) {
			TSocket socket = new TSocket(TAG_IP, TAG_PORT);
			socket.setTimeout(TIMEOUT);
			TTransport transport = socket;
			try {
				TProtocol protocol = new TCompactProtocol(transport);
				LabelBaseDumpService.Client client = new LabelBaseDumpService.Client(protocol);
				transport.open();
				Map<String, Long> dumptag = (HashMap<String, Long>)client.dump_light_string(typelist);
				return dumptag;
			} catch(Exception ex) {
				System.err.print("retry = " + retry + "\n" + ex.toString());
				try {
					Thread.sleep(SLEEP);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} finally {
				retry ++;
				transport.close();
			}
		}
	}
	
	public Map<String, Tag> DumpTypeString(short type)  throws TException{
		
		int retry = 0;
		while(true) {
			TSocket socket = new TSocket(TAG_IP, TAG_PORT);
			socket.setTimeout(TIMEOUT);
			TTransport transport = socket;
			try {
				TProtocol protocol = new TCompactProtocol(transport);
				LabelBaseDumpService.Client client = new LabelBaseDumpService.Client(protocol);
				transport.open();
				Map<String, Tag> dumptag = (HashMap<String, Tag>)client.dumptype_string(type);
				return dumptag;
			} catch(Exception ex) {
				System.err.print("retry = " + retry + "\n" + ex.toString());
				try {
					Thread.sleep(SLEEP);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} finally {
				retry ++;
				transport.close();
			}
		}
	}
	
	public static void main(String[] args) {
		
		String output_path = "user_tags.txt";
		int level = -1;
		if(args.length >= 2)
		{
			level = Integer.parseInt(args[0]);
			output_path = args[1];
		}
		
		System.out.println("Level:" + level);
		System.out.println("Output:" + output_path);
		
		UserTagsDumper d = new UserTagsDumper();
		long time1 = System.currentTimeMillis();
		Map<Long, Tag> hash = d.DumpLong(null);
		long time2 = System.currentTimeMillis();
		System.out.println(time2 - time1);
		System.out.println(hash.size());
		
		//weight is zero for all tags in taglib
		try {
			FileWriter fw = new FileWriter(output_path);
			Set<Long> kset = hash.keySet();
			for(java.util.Iterator<Long> iter = kset.iterator();iter.hasNext();)
			{
				Long key = iter.next();
				Tag tag = (Tag)hash.get(key);
				if(level == -1)
					//fw.write(key + ":" + tag.fatherid + ":" + tag.display + ":" + tag.wordgroup + "\n");
					fw.write(key + "\t" + tag.wordgroup + "\n");
				else if(level == 0)
				{
					if(tag.fatherid == 0)
						fw.write(key + "\t" + tag.wordgroup + "\n");
				}
				else if(level == 1)
				{
					if(tag.fatherid != 0 && (tag.fatherid & 0x0000ffffffff0000l) == 0l)
						fw.write(key + "\t" + tag.wordgroup + "\n");
				}
			}
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Finished");
	}
}

