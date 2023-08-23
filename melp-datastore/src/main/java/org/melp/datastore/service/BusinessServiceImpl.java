package org.melp.datastore.service;

import java.util.Collection;

import org.melp.datastore.model.Business;
import org.melp.datastore.model.BusinessRepository;
import org.springframework.stereotype.Service;

@Service
public class BusinessServiceImpl implements BusinessService {

	private final BusinessRepository repo;
	
	public BusinessServiceImpl(final BusinessRepository repo){
		this.repo = repo;
	}
	
	@Override
	public Business saveBusiness(Business business) {
		return repo.save(business);
	}

	@Override
	public Business getBusiness(String business_id) {
		return repo.getReferenceById(business_id);
	}
}
