package adbi.insertdb;

public class InsertDB {
	public static void main(String[] args){
		String time = null;
		String timeInputPath = null;
		String regionInputPath = null;
		String uvInputPath = null;
		if(args.length != 4){
			System.out.println("agrs format wrong");
			return;
		}
		if(args.length == 4){
			timeInputPath = args[0];
			regionInputPath = args[1];
			uvInputPath = args[2];
			time = args[3];
		}
		AdInsertRegion insertRegion = new AdInsertRegion(time,regionInputPath);
		insertRegion.init();
		insertRegion.insert();
		insertRegion.cleanup();
		
		AdInsertTime insertTime = new AdInsertTime(time,timeInputPath);
		insertTime.init();
		insertTime.insert();
		insertTime.cleanup();
		
		AdInsertUV insertUV = new AdInsertUV(time,uvInputPath);
		insertUV.init();
		insertUV.insert();
		insertUV.cleanup();
		
//		AdInsertInterest insertInterest = new AdInsertInterest("2013/05/26","files");
//		insertInterest.init();
//		insertInterest.insert();
//		insertInterest.cleanup();
//		
		
	}
}
