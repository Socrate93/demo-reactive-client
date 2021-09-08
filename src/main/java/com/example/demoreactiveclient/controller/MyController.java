package com.example.demoreactiveclient.controller;

import org.reactivestreams.Publisher;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/client")
public class MyController {

  private final WebClient webClient;

  public MyController() {
    this.webClient = WebClient.create("http://localhost:9000");
  }

  @GetMapping(value = "/products", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public Flux<Product> getProducts() {
    return webClient.get()
            .uri("/products")
            .retrieve()
            .bodyToFlux(Product.class)
            .concatMap(this::fetchRelated)
            .doOnNext(System.out::println);
  }

  private Mono<Product> fetchRelated(Product product) {
    return webClient.get()
            .uri("/products/related-to/"+product.getId())
            .retrieve()
            .bodyToFlux(Product.class)
            .filterWhen(this::fetchStock)
            .collectList()
            .map(related -> new Product(product.getId(), product.getName(), related));
  }

  private Mono<Boolean> fetchStock(Product product) {
    return webClient.get()
            .uri("/products/"+product.getId()+"/stocks")
            .retrieve()
            .bodyToMono(Integer.class)
            .map(stock -> stock > 0);
  }
}
