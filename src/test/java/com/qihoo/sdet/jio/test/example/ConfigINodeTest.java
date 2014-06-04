package com.qihoo.sdet.jio.test.example;

import junit.framework.TestCase;

import com.qihoo.sdet.jio.api.core.Config;
import com.qihoo.sdet.jio.api.core.IConfig.INode;

public class ConfigINodeTest extends TestCase
{	
	public void testGetINode() throws InterruptedException
	{
		Config.getInstance().loadConf("conf.xml");
		String name = Config.getInstance().getFirstNode("server").getName();
		assertEquals(name, "server");
		String attribute = Config.getInstance().getFirstNode("server").getAttribute().get("port");
		assertEquals(attribute, "11111");
		INode nullName = Config.getInstance().getFirstNode("null");
		assertNull(nullName);
	}
}
