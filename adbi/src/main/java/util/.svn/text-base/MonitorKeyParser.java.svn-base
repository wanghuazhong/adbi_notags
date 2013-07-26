package util;

public class MonitorKeyParser {
	private String m_monitor_key;
	
	private long m_source_id;
	private long m_advertiser_id;
	private long m_campaign_id;
	private long m_adgroup_id;
	private long m_line_id;
	private long m_material_id;
	
	public MonitorKeyParser(){
		m_monitor_key = "";
		m_source_id = m_advertiser_id = m_campaign_id = m_adgroup_id = m_line_id = m_material_id = 0l;
	}
	
	public MonitorKeyParser(String monitory_key){
		m_monitor_key = monitory_key;
		m_source_id = m_advertiser_id = m_campaign_id = m_adgroup_id = m_line_id = m_material_id = 0l;
	}
	
	static long parse62Digit(char ch) throws ClassCastException
	{
		long res = 0l;
		if(ch >= '0' && ch <= '9')
			res = ch - '0';
		else if(ch >= 'a' && ch <= 'z')
			res = ch - 'a' + 10;
		else if(ch >= 'A' && ch <= 'Z')
			res = ch - 'A' + 36;
		else
			throw new ClassCastException("parse62Digit error:" + ch);
		
		return res;
	}
	
	static long parse62Base(String str) throws ClassCastException
	{
		long res = 0l;
		for(int i = 0;i < str.length();++i)
		{
			try
			{
				res = res * 62 + parse62Digit(str.charAt(i));
			}
			catch(ClassCastException e)
			{
				throw e;
			}
		}
		return res;
	}
	
	public boolean parseMonitorKey()
	{
		if(m_monitor_key == null)
			return false;
		
		if(m_monitor_key.length() != ConstData.MONITOR_KEY_LENGTH)
		{
//			System.err.println("monitory key length error:" + m_monitor_key.length() + ",key:" + m_monitor_key);
			return false;
		}
		
		try
		{
			int begIdx = 0;
			int endIdx = ConstData.MONITOR_KEY_FIELD_LENGTH.get(ConstData.MK_SOURCE);
			String tmp = m_monitor_key.substring(begIdx, endIdx);
			m_source_id = parse62Base(tmp);
			
			begIdx = endIdx;
			endIdx += ConstData.MONITOR_KEY_FIELD_LENGTH.get(ConstData.MK_ADVERTISER);
			tmp = m_monitor_key.substring(begIdx, endIdx);
			m_advertiser_id = parse62Base(tmp);
			
			begIdx = endIdx;
			endIdx += ConstData.MONITOR_KEY_FIELD_LENGTH.get(ConstData.MK_COMPAIGN);
			tmp = m_monitor_key.substring(begIdx, endIdx);
			m_campaign_id = parse62Base(tmp);
			
			begIdx = endIdx;
			endIdx += ConstData.MONITOR_KEY_FIELD_LENGTH.get(ConstData.MK_ADGROUP);
			tmp = m_monitor_key.substring(begIdx, endIdx);
			m_adgroup_id = parse62Base(tmp);
			
			begIdx = endIdx;
			endIdx += ConstData.MONITOR_KEY_FIELD_LENGTH.get(ConstData.MK_LINE);
			tmp = m_monitor_key.substring(begIdx, endIdx);
			m_line_id = parse62Base(tmp);
			
			begIdx = endIdx;
			endIdx += ConstData.MONITOR_KEY_FIELD_LENGTH.get(ConstData.MK_MATERIAL);
			tmp = m_monitor_key.substring(begIdx, endIdx);
			m_material_id = parse62Base(tmp);
		}
		catch(ClassCastException e)
		{
			System.err.println("Exception:" + e.getMessage());
			return false;
		}
		return true;
	}
	
	
	public String get_monitor_key() {
		return m_monitor_key;
	}

	public void set_monitor_key(String monitor_key) {
		m_monitor_key = monitor_key;
	}

	public long get_source_id() {
		return m_source_id;
	}

	public long get_advertiser_id() {
		return m_advertiser_id;
	}

	public long get_campaign_id() {
		return m_campaign_id;
	}

	public long get_adgroup_id() {
		return m_adgroup_id;
	}

	public long get_line_id() {
		return m_line_id;
	}

	public long get_material_id() {
		return m_material_id;
	}

	public String toString()
	{
		return m_monitor_key + "->" + "S:" + m_source_id + " A:" + m_advertiser_id + " C:" + m_campaign_id 
				+ " G:" + m_adgroup_id + " L:" + m_line_id + " M:" + m_material_id;
	}
	
	public static void main(String[] args) {
		MonitorKeyParser mkp = new MonitorKeyParser();
		mkp.set_monitor_key("20q1d000r0000000q2R000q79");
		if(mkp.parseMonitorKey())
		{
			System.out.println(mkp.toString());
		}
		else
		{
			System.out.println("fail to parse monitoryKey");
		}
		
		mkp.set_monitor_key("2001D00030001D00q1z000q1?");
		if(mkp.parseMonitorKey())
		{
			System.out.println(mkp.toString());
		}
		else
		{
			System.out.println("fail to parse monitoryKey");
		}
	}
}
