package com.appsdeveloperblog.ws.emailnotification.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import com.appsdeveloperblog.ws.core.ProductCreatedEvent;
import com.appsdeveloperblog.ws.emailnotification.error.NotRetryableException;
import com.appsdeveloperblog.ws.emailnotification.error.RetryableException;

import org.springframework.util.MultiValueMap;
import org.springframework.util.LinkedMultiValueMap;

@Component
@KafkaListener(topics = "product-created-events-topic")
public class ProductCreatedEventHandler {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
	private RestTemplate restTemplate;

	public ProductCreatedEventHandler(RestTemplate restTemplate) {
		this.restTemplate = restTemplate;
	}

	@KafkaHandler
	public void handle(ProductCreatedEvent productCreatedEvent) {

		// force to throw exception for testing
		//if(true) throw new NotRetryableException("An error took place. No need to consume this message");

		LOGGER.info("Received a new event: " + productCreatedEvent.getTitle() + " with productId: "
				+ productCreatedEvent.getProductId());

		String requestUrl = "http://localhost:8082/response/200";

		try {

			HttpHeaders headers = new HttpHeaders();
			headers.set("Authorization", "Bearer eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJ5OTEyM0liRFRoYllfQ1hONjF2UEhUeVloWE5XOWZULTZUSzNtemp5Z0JNIn0.eyJleHAiOjE3MDc4MDY2NDAsImlhdCI6MTcwNzgwNjM0MCwianRpIjoiYjAzOTA3NmItNGRiNS00ZDBmLWEyMTAtOWQ5ZmFjNDlhZjZiIiwiaXNzIjoiaHR0cDovL2xvY2FsaG9zdDo4MDAwL3JlYWxtcy9hcHBzZGV2ZWxvcGVyYmxvZyIsImF1ZCI6ImFjY291bnQiLCJzdWIiOiJhMDg5NTgxYS1lZDc4LTQxYTktOGIyOS0wMmU2OWUyMzc5YzEiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJwaG90by1hcHAtY29kZS1mbG93LWNsaWVudCIsInNlc3Npb25fc3RhdGUiOiJlMDVhNzBkOC05OWIxLTQ5ZjktYjUwZC04YjQ5YmJiNzFlOTIiLCJhY3IiOiIxIiwiYWxsb3dlZC1vcmlnaW5zIjpbIiJdLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsiZGVmYXVsdC1yb2xlcy1hcHBzZGV2ZWxvcGVyYmxvZyIsIm9mZmxpbmVfYWNjZXNzIiwiZGV2ZWxvcGVyIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6eyJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6Im9wZW5pZCBlbWFpbCBwcm9maWxlIiwic2lkIjoiZTA1YTcwZDgtOTliMS00OWY5LWI1MGQtOGI0OWJiYjcxZTkyIiwiZW1haWxfdmVyaWZpZWQiOmZhbHNlLCJuYW1lIjoiZmlyc3RuYW1lIGxhc3RuYW1lIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiZGV2IiwiZ2l2ZW5fbmFtZSI6ImZpcnN0bmFtZSIsImZhbWlseV9uYW1lIjoibGFzdG5hbWUiLCJlbWFpbCI6ImVAbWFpbC5jb20ifQ.dOEwtN24WTqhPyASJQKI_jn1bFB7bipwwUoN3Ule5ou6uZ9tl5At_3wVtsVlPpLA32WQWGTB_Q7NsqKATYLFNg-dtln4Ax74D7vUHfJA6P2rdoW8nOTCr8_HA0aTNI60hgA_8myyZuS9b4S6uMhTpfs-qgONC03Or9BXvGiIbR4OxXEpYu96UbPslXuyta08HEbfJEU77JTZF0elbqBgEuG9Q_dzSBuzZrAZ-2k3TYshx0tiWwqYNTxe2C7fWSoHBOFSNSguDJow-BYrQFar1SIiMTpl9W_5-oGAhZe8mM4d7lAIM9YLRI4rs9q2UXFqvC1pVR7_QiYPI2rmp7JSzw"); //accessToken can be the secret key you generate.
			headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
			HttpEntity <String> entity = new HttpEntity <> (headers);
			ResponseEntity <String> response = restTemplate.exchange(requestUrl, HttpMethod.GET, entity, String.class);

			//ResponseEntity<String> response = restTemplate.exchange(requestUrl, HttpMethod.GET, null, String.class);
			if (response.getStatusCode().value() == HttpStatus.OK.value()) {
				LOGGER.info("Received response from a remote service: " + response.getBody());
			}
		} catch (ResourceAccessException ex) {
			LOGGER.error(ex.getMessage());
			throw new RetryableException(ex);
		} catch (HttpServerErrorException ex) {
			LOGGER.error(ex.getMessage());
			throw new NotRetryableException(ex);
		} catch (Exception ex) {
			LOGGER.error(ex.getMessage());
			throw new NotRetryableException(ex);
		}

	}

}
