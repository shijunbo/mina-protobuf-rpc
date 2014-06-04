package com.qihoo.sdet.jio.api.core;

import java.util.List;
import java.util.Map;

public interface IConfig {
	public interface INode {
		Map<String, String> getAttribute();

		String getName();

		List<INode> getChildren();
	}

	void loadConf(String conf);

}
