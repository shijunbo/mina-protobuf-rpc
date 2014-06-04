package com.qihoo.sdet.jio.test.example;

import java.io.File;
import java.io.FilenameFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qihoo.sdet.jio.api.client.TcpClient;
import com.qihoo.sdet.jio.api.core.Config;
import com.qihoo.sdet.jio.api.core.RpcClientChannel;


@SuppressWarnings("rawtypes")
public class StreamClient extends TcpClient {

	private static final Logger LOGGER = LoggerFactory.getLogger(StreamClient.class);
	static File[] collectApk(String resPath)
	{
		File resDir = new File(resPath);
		
		File[] apks = resDir.listFiles(
				new FilenameFilter()
				{
					@Override
					public boolean accept(File dir, String name)
					{
						if (name.endsWith(".apk"))
						{
							return true;
						}
						return false;
					}
				});
		return apks;
	}
	
	public static void main(String[] args) throws Exception {
		
		Config.getInstance().loadConf("conf.xml");
		
		while(true){
			Thread.sleep(5000);
		}
	}
	
	public static void sendFile(RpcClientChannel rpcChannel){
		
		File []  fs = collectApk("src/test/resources");
		for (File f: fs){
			try {
				LOGGER.info("send file {} with peer {}" , f.getAbsolutePath(), rpcChannel.synStream(f.getAbsolutePath()) );
			} catch ( Exception e ) {
				e.printStackTrace();
				LOGGER.error("sending failed: " + e.getMessage());
			}
		}
	}
	
	
	public void sessionClosed(RpcClientChannel rpcChannel){
		LOGGER.info("sessionClosed ..." + rpcChannel.getSession());
	}
	
	public void sessionOpen(RpcClientChannel rpcChannel){
		LOGGER.info("sessionOpen ..."+ rpcChannel.getSession());
		sendFile(rpcChannel);
	}
}
