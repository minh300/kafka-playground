package com.melp.checkin.controller;

import java.io.IOException;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.melp.checkin.service.CheckinService;

import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/checkin")
public class CheckinController {

	private final CheckinService checkinService;

	public CheckinController(final CheckinService checkinService) {
		this.checkinService = checkinService;
	}

	@PostMapping
	public Mono<ResponseEntity<Object>> checkin(
			@RequestParam("lat") final long latitude,
			@RequestParam("long") final long longitude) throws IOException {

		return checkinService.checkin(latitude, longitude)
				.then(Mono.just(ResponseEntity.ok().build()));
	}
}
