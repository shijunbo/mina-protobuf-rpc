package com.qihoo.sdet.jio.api.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.Service;

public class RpcServiceRegistry {

	private List<Service> serviceImplementations = new ArrayList<Service>();

	private Map<String, Service> serviceNameMap = new HashMap<String, Service>();

	public RpcServiceRegistry() {
	}

	public void registerService(Service serviceImplementation) {
		String serviceName = serviceImplementation.getDescriptorForType()
				.getName();
		if (serviceNameMap.containsKey(serviceName)) {
			throw new IllegalStateException("Duplicate serviceName "
					+ serviceName);
		}
		serviceNameMap.put(serviceName, serviceImplementation);
		serviceImplementations.add(serviceImplementation);
	}

	public Service resolveService(String serviceName) {
		return serviceNameMap.get(serviceName);
	}

}
