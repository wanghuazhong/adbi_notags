package util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Properties;

import javax.activation.DataHandler;
import javax.activation.FileDataSource;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

public class MailUtil {
	
	/**
	 * send e-mail
	 * @param from
	 * @param to
	 * @param subject
	 * @param text
	 * @param attachments
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	public String send(String from, String to, String subject, String text, String[] attachments) throws UnsupportedEncodingException {
		
		String server = "10.11.156.63";
		Properties pro = new Properties();
		pro = System.getProperties();
		pro.put(server, "true");
		pro.put("autodetectip", "true");
		pro.put("autodetect", "false");
		Session mailsession = Session.getDefaultInstance(pro);
		try {
			Message msg = new MimeMessage(mailsession);
			msg.setFrom(new InternetAddress(from));
			msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
			msg.setSubject(subject);
			//msg.setText(text);
			msg.setSentDate(new Date());
			Multipart mp = new MimeMultipart();   
			MimeBodyPart body = new MimeBodyPart();  
			body.setContent(text, "text/html;charset=gb2312");
			mp.addBodyPart(body);
			if(attachments != null && attachments.length > 0) {
				
				for(String attachment : attachments) {
					MimeBodyPart mbp = new MimeBodyPart();   
					String filename = attachment;
					FileDataSource fds = new FileDataSource(filename);
					mbp.setDataHandler(new DataHandler(fds));
					mbp.setFileName(fds.getName());
					mp.addBodyPart(mbp);
				}
			}
			msg.setContent(mp);
			Transport ps = mailsession.getTransport("smtp");
			ps.connect(server, "", "");
			ps.sendMessage(msg, InternetAddress.parse(to));
			ps.close();
			return "success!";
		} catch (MessagingException se) {
			System.out.println(se.getMessage());
			se.printStackTrace();
		}
		return "failed!";
	}
	

	public static void main(String[] args) {
		if (args.length < 5) {
			System.err.println("Usage:mailto attachments contentfile mailfrom title");
			System.exit(-1);
		}
		MailUtil mu = new MailUtil();
		String to = args[0];
		String[] attachments = args[1].split(",",-1);
		//String[] attachments = null;
		String mailfrom = args[3];
		String title = args[4];
		
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(args[2]), "utf-8"));
			String line;
			String text = "";
			while ((line = reader.readLine()) != null) {
				text += line;
				text += "<br>";
			}
			reader.close();
			//mu.send("AD_BI", to, "Daily_Report", text, attachments);
			mu.send(mailfrom, to, title, text, attachments);
		} catch (UnsupportedEncodingException ex) {
			ex.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("Email done.");
	}
}
