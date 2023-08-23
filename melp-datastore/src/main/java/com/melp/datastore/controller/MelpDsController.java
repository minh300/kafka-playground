package com.melp.datastore.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.melp.datastore.model.Business;
import com.melp.datastore.service.BusinessService;

@RestController
public class MelpDsController {
	private final BusinessService service;
	
	public MelpDsController(final BusinessService businessService) {
		this.service = businessService;
	}
	
	@PostMapping("/businesses")
	public Business newEmployee(@RequestBody Business newBusiness) {
		return service.saveBusiness(newBusiness);
	}

}
