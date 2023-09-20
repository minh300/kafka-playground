package com.melp.checkin.service;

import reactor.core.publisher.Mono;

public interface CheckinService {
	public Mono<Void> checkin(final float latitude, final float longitude);
}
