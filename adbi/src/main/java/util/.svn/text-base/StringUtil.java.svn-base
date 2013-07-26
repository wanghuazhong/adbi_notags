package util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtil {
	
	public static boolean regex(String regex, String str) {
		Pattern p = Pattern.compile(regex, Pattern.MULTILINE);
		Matcher m = p.matcher(str.toUpperCase());
		return m.find();
	}
	
	public static boolean isBlank(String str) {
		return ((str == null) || (str.length() == 0) || (str.trim().length() == 0));
	}
	
	public static String parseUrl(String url) {
		int indexOf = url == null || url.trim().length() == 0 ? -1 : url
				.indexOf("?");

		if (indexOf == -1) {
			return url;
		}
		url = url.substring(0, indexOf);
		
		String regex=".(shtml|html|htm)#\\d*";
		
		return url.replaceAll(regex, "");
	}

	public static String parseDomain(String url) {
		// Pattern p = Pattern.compile(
		// "[^//]*?\\.(com|cn|net|org|biz|info|cc|tv)", 2);
		// String protocol = "(?:(mailto|ssh|ftp|https?)://)?";
		String hostname = "(?:[a-z0-9](?:[-a-z0-9]*[a-z0-9])?\\.)+(?:com|cn|net|edu|biz|gov|org|info|cc|tv|in(?:t|fo)|(?-i:[a-z][a-z]))";
		String ip = "(?:[01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.(?:[01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.(?:[01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.(?:[01]?\\d\\d?|2[0-4]\\d|25[0-5])";
		String port = "(?::(\\d{1,5}))?";
		String pattern = "((?:" + hostname + "|" + ip + "))" + port;
		Pattern p = Pattern.compile(pattern);
		Matcher matcher = p.matcher(url);
		if (matcher.find()) {
			return matcher.group();
		}
		return null;
	}

	public static boolean isUrl(String url) {
		// Pattern p = Pattern.compile(
		// "[^//]*?\\.(com|cn|net|org|biz|info|cc|tv)", 2);
		// String protocol = "(?:(mailto|ssh|ftp|https?)://)?";
		if (url != null && url.trim().length() > 0) {
			String hostname = "(?:[a-z0-9](?:[-a-z0-9]*[a-z0-9])?\\.)+(?:com|cn|net|edu|biz|gov|org|info|cc|tv|in(?:t|fo)|(?-i:[a-z][a-z]))";
			String ip = "(?:[01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.(?:[01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.(?:[01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.(?:[01]?\\d\\d?|2[0-4]\\d|25[0-5])";
			String port = "(?::(\\d{1,5}))?";
			String pattern = "((?:" + hostname + "|" + ip + "))" + port;
			Pattern p = Pattern.compile(pattern);
			Matcher matcher = p.matcher(url);
			if (matcher.find()) {
				return true;
			}
			return false;
		} else
			return false;

	}

	public static String split(String region) {

		if (region != null&&region.length()>=4) {// ż��ȵĵ��򲻴���
			String[] result = new String[region.length() / 2 - 1];
			result[0] = region.substring(0, 4);
			if (region.length() >= 6) {
				result[1] = region.substring(0, 6);
				return result[0] + "-" + result[1];
			}
			return result[0] + "-" + result[0] + "00";
		} else if ((region == null || region.trim().length() == 0)
				|| (region != null && region.equalsIgnoreCase("unknow"))) {
			return "unknow";
		} else {
			return region;
		}
	}
}
