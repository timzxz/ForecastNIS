package CommonTools;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.conn.params.ConnRouteParams;
import org.apache.http.impl.client.DefaultHttpClient;

import Configure.Param;


public class TestConnection {
	
	public static boolean isConnection(){
		HttpClient httpclient = new DefaultHttpClient();
		HttpHost proxy = new HttpHost(ProxyHostIP,ProxyPort);  
		httpclient.getParams().setParameter(ConnRouteParams.DEFAULT_PROXY, proxy);
		try{
			HttpGet get = new HttpGet("https://www.google.com");
			HttpResponse re_response = httpclient.execute(get);
		}catch(Exception e){
			System.out.println("代理IP  "+ProxyHostIP+":"+ProxyPort+"  重连中....");
			return false;
		}
		return true;
		
	}
	
	public static void main(String ...strings){
		System.out.println(isConnection());
	}
}
