package com.melp.event.processor;

public class ProcessorUtil {
	
	public static IEventProccesor fromString(final String type) {
		switch(type) {//TODO
			case BusinessProcessor.BUSINESS_TOPIC: return new BusinessProcessor(); 
			case "chechkin":  return new BusinessProcessor(); 
			case "review":  return new BusinessProcessor(); 
			default:
		}
		return null;
	}
}
