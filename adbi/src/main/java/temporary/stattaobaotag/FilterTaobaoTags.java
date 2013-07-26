package temporary.stattaobaotag;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class FilterTaobaoTags {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String inpath = "D:\\Work\\Ecllipse\\ad_bi\\src\\temporary\\stattaobaotag\\tbk-keyword-tagid.txt";
		String outpath = "D:\\Work\\Ecllipse\\ad_bi\\src\\temporary\\stattaobaotag\\tbk_user_tags_white_list.txt";
		try {
			BufferedReader bf = new BufferedReader(new InputStreamReader(new FileInputStream(inpath)));
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outpath)));
			String line;
			long prg = 0;
			long type_mask = 0xffff000000000000l;
			long tag_mask = 0xffffffffffff0000l;
			while(true)
			{
				line = bf.readLine();
				if(line == null)
					break;
				
				String[] tags = line.split(",");
				for(int i = 0;i < tags.length;++i)
				{
					if(tags[i].equals(""))
						continue;
					
					long tag = Long.valueOf(tags[i]);
					if((tag&type_mask) != (6l<<48))
						continue;
					
					bw.write((tag&tag_mask) + "\t" + "null");
					bw.newLine();
					
					++prg;
					if(prg % 1000 == 0)
						System.out.println("prg:" + prg);
				}
			}
			bf.close();
			bw.close();
			System.out.println("all prg:" + prg);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
