package com.melp.datastore.service;

import com.melp.datastore.model.Business;

public interface BusinessService {
	public Business saveBusiness(final Business business);
	public Business getBusiness(final String business_id);
}
