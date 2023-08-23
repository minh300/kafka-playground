package org.melp.datastore.service;

import org.melp.datastore.model.Business;

public interface BusinessService {
	public Business saveBusiness(final Business business);
	public Business getBusiness(final String business_id);
}
