package adbi.test;

import java.util.ArrayList;
import java.util.List;

import com.sohu.ADRD.AudienceTargeting.io.redis.UserAttributeReader;
import com.sohu.ADRD.AudienceTargeting.io.redis.res;

public class testUAReader {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String ip = "192.168.230.95";
		int port = 9090;
		UserAttributeReader uar = null;
		try {
			uar = new UserAttributeReader(ip,port);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String user = "abc";
		List<String> userList = new ArrayList<String>();
		for(int i = 0;i < 1000;++i)
			userList.add("abc" + i);
		
		long start,finish;
		//String user = "CE710708E9C71800C7D4BCA31AB890FA";
		try {
			start = System.currentTimeMillis();
			for(int i = 0;i < 1000;++i)
			{
				user = userList.get(i);
				res tags = uar.getTags(user);
				if(tags == null)
					System.out.println("tags null");
				else
				{
					System.out.print("key:" + tags.okey);
					List<Long> numtags = tags.oval;
					System.out.println("num:" + numtags.size());
//					Iterator<Long> iter = numtags.iterator();
//					while(iter.hasNext())
//						System.out.print(iter.next() + ",");
//					System.out.println();
				}
			}
			finish = System.currentTimeMillis();
			System.out.println("ms:" + (finish - start));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
