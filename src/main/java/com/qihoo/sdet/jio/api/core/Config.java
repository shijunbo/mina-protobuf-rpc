package com.qihoo.sdet.jio.api.core;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;

import com.qihoo.sdet.jio.api.client.TcpClient;
import com.qihoo.sdet.jio.api.server.TcpServer;

public class Config implements IConfig {

	private List<INode> clientNodes;
	private List<INode> serverNodes;
	private List<INode> allNodes;

	private volatile static Config config;

	static public class INodeImpl implements INode {

		private List<INode> nodes;
		private String name;
		private Map<String, String> attribute;

		public INodeImpl() {
			attribute = new HashMap<String, String>();
			nodes = new ArrayList<INode>();
		}

		@Override
		public Map<String, String> getAttribute() {
			return attribute;
		}

		@Override
		public String getName() {
			return name;
		}

		@Override
		public List<INode> getChildren() {
			return nodes;
		}
	}

	public static Config getInstance() {
		if (config == null) {
			synchronized (Config.class) {
				if (config == null) {
					config = new Config();
				}
			}
		}
		return config;
	}

	private Config() {
		clientNodes = new ArrayList<INode>();
		serverNodes = new ArrayList<INode>();
		allNodes = new ArrayList<IConfig.INode>();
	}

	public INode getFirstNode(String nodeName) {
		INode n = null;
		for (INode node : allNodes) {
			if (node.getName().equals(nodeName)) {
				n = node;
			}
		}
		return n;
	}

	public List<INode> getNodes(String nodeName) {
		List<INode> result = new ArrayList<IConfig.INode>();
		for (INode node : allNodes) {
			result.add(node);
		}

		return result;
	}

	static public class XMLHandler extends DefaultHandler {
		private Map<String, Boolean> attris = new HashMap<String, Boolean>();
		private StringBuffer buf = new StringBuffer();

		public void startDocument() throws SAXException {
		}

		public void endDocument() throws SAXException {
		}

		public void startPrefixMapping(String prefix, String uri) {
		}

		public void endPrefixMapping(String prefix) {
		}

		public void startElement(String namespaceURI, String localName,
				String fullName, Attributes attributes) throws SAXException {

			attris.clear();
			INodeImpl il = new INodeImpl();
			il.name = fullName;
			Config.getInstance().addNodes(fullName, il);

			for (int i = 0; i < attributes.getLength(); i++) {
				String attriName = attributes.getLocalName(i);
				il.attribute.put(attriName, attributes.getValue(i));
				attris.put(attriName, true);
			}
		}

		public void endElement(String namespaceURI, String localName,
				String fullName) throws SAXException {
			buf.setLength(0);
		}

		public void characters(char[] chars, int start, int length)
				throws SAXException {
			buf.append(chars, start, length);
		}

		public void warning(SAXParseException exception) {
		}

		public void error(SAXParseException exception) throws SAXException {
			throw new ConfigException("ERROR. line: "
					+ exception.getLineNumber() + " colum: "
					+ exception.getColumnNumber() + " msessage: "
					+ exception.getMessage());
		}

		public void fatalError(SAXParseException exception) throws SAXException {
			throw new ConfigException("FATAL ERROR. line: "
					+ exception.getLineNumber() + " colum: "
					+ exception.getColumnNumber() + " msessage: "
					+ exception.getMessage());
		}
	}

	public boolean isClient(String type) {
		return getClientInode(type) != null;
	}

	public String getClientHost(String type) {
		INode i = getClientInode(type);
		return i.getAttribute().get("host");
	}

	public String getClientPort(String type) {
		INode i = getClientInode(type);
		return i.getAttribute().get("port");
	}

	public boolean isServer(String type) {
		return getServerInode(type) != null;
	}

	public String getServerHost(String type) {
		INode i = getServerInode(type);
		return i.getAttribute().get("host");
	}

	public String getServerPort(String type) {
		INode i = getServerInode(type);
		return i.getAttribute().get("port");
	}

	private INode getClientInode(String type) {
		for (INode i : clientNodes)
			if (i.getAttribute().get("type").equals(type))
				return i;
		return null;
	}

	private INode getServerInode(String type) {
		for (INode i : serverNodes)
			if (i.getAttribute().get("type").equals(type))
				return i;
		return null;
	}

	public void addNodes(String name, INodeImpl i) {
		if ("client".equalsIgnoreCase(name))
			clientNodes.add(i);
		else if ("server".equalsIgnoreCase(name))
			serverNodes.add(i);
		allNodes.add(i);
	}

	@Override
	public void loadConf(String conf) {
		InputSource is = new InputSource(getClass().getClassLoader()
				.getResourceAsStream(conf));

		try {
			loadLogBack(conf);
			loadConf(is);
			System.out.println("init config ok...");
		} catch (Exception e) {
			throw new ConfigException("Init config Failure:" , e);
		}
	}

	public void loadConf(InputSource is) throws SAXException, IOException,
			ParserConfigurationException {
		SAXParserFactory factory = SAXParserFactory.newInstance();
		factory.setNamespaceAware(false);
		DefaultHandler defaultHandler = new XMLHandler();
		factory.newSAXParser().parse(is, defaultHandler);
		init();
	}

	public void loadLogBack(String conf) throws Exception {
		LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
		JoranConfigurator configurator = new JoranConfigurator();
		configurator.setContext(lc);
		lc.reset();
		InputStream is = Config.class.getClassLoader()
				.getResourceAsStream(conf);
		try {
			DocumentBuilder builder = DocumentBuilderFactory.newInstance()
					.newDocumentBuilder();
			Document document = builder.parse(is);
			Element rElement = document.getDocumentElement();
			if (!rElement.getNodeName().equals("configuration")) {
				throw new ConfigException(
						"The root Element must be <configuration> ");
			}
			is = Config.class.getClassLoader().getResourceAsStream(conf);
			configurator.doConfigure(is);
		} catch (Exception e) {
			throw e;
		}
	}

	private void init() {
		try {
			for (INode node : serverNodes) {
				TcpServer<?> server = (TcpServer<?>) (Class.forName(
						node.getAttribute().get("type")).getConstructors()[0]
						.newInstance());
				server.initRpcService(server);

			}
			for (INode node : clientNodes) {
				TcpClient<?> client = (TcpClient<?>) (Class.forName(
						node.getAttribute().get("type")).getConstructors()[0]
						.newInstance());
				client.initRpcService(client);
			}
		} catch (Exception e) {
			throw new ConfigException("Init config Failure:", e);
		}

	}
}
