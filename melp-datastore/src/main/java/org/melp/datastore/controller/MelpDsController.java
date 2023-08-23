package org.melp.datastore.controller;

import org.melp.datastore.model.Business;
import org.melp.datastore.service.BusinessService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

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
