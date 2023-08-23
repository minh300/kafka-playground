package com.melp.datastore.service;

import java.util.Collection;

import org.springframework.stereotype.Service;

import com.melp.datastore.model.Business;
import com.melp.datastore.model.BusinessRepository;

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
