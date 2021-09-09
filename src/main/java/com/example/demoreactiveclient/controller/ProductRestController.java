package com.example.demoreactiveclient.controller;

import lombok.Data;
import org.reactivestreams.Publisher;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

@RestController
@RequestMapping("/client")
public class ProductRestController {

  private final WebClient webClient;
  private final WebClient stockWebClient;
  private final WebClient relatedWebClient;

  public ProductRestController() {

    ConnectionProvider connProvider = ConnectionProvider
                                       .builder("webclient-conn-pool")
                                        .maxConnections(1000)
                                        .maxIdleTime(Duration.ofMillis(20000))
                                        .maxLifeTime(Duration.ofMillis(300_000))
                                        .pendingAcquireMaxCount(8000)
                                        .pendingAcquireTimeout(Duration.ofMillis(10_000))
                                        .build();
    HttpClient nettyHttpClient = HttpClient
            .create(connProvider)
            .wiretap(Boolean.TRUE);

    this.webClient = WebClient.builder()
            .clientConnector(new ReactorClientHttpConnector(nettyHttpClient))
            .baseUrl("http://localhost:9000")
            .build();
    this.stockWebClient = WebClient.builder()
            .clientConnector(new ReactorClientHttpConnector(nettyHttpClient))
            .baseUrl("http://localhost:9000")
            .build();
    this.relatedWebClient = WebClient.builder()
            .clientConnector(new ReactorClientHttpConnector(nettyHttpClient))
            .baseUrl("http://localhost:9000")
            .build();
  }

  @GetMapping(value = "/products", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public Flux<Product> getProducts() {
    long start = System.currentTimeMillis();
    return getProductFlux()
            //.flatMap(this::fetchRelated)
            .collectList()
            .flatMapMany(this::fetchRelated)
            .doOnComplete(() -> System.out.println("Reactive : " + (System.currentTimeMillis() - start)));
  }

  private Flux<Product> getProductFlux() {
    return webClient.get()
            .uri("/products")
            .retrieve()
            .bodyToFlux(Product.class);
  }

  private Mono<Product> fetchRelated(Product product) {
    return relatedWebClient.get()
            .uri("/products/"+product.getId()+"/related")
            .retrieve()
            .bodyToFlux(Product.class)
            .filterWhen(this::fetchStock)
            .collectList()
            .map(related -> new Product(product.getId(), product.getName(), related));
  }

  private Mono<Boolean> fetchStock(Product product) {
    return stockWebClient.get()
            .uri("/products/"+product.getId()+"/stocks")
            .retrieve()
            .bodyToMono(Integer.class)
            .map(stock -> stock > 0);
  }

  // V2
  private Flux<Product> fetchRelated(List<Product> products) {
    Map<String, Product> mappedProducts = products.stream().collect(Collectors.toMap(Product::getId, Function.identity()));
    return relatedWebClient.get()
            .uri(uriBuilder -> uriBuilder
                    .path("/products/related")
                    .queryParam("ids", products.stream().map(Product::getId).collect(toSet()))
                    .build())
            .retrieve()
            .bodyToFlux(ProductWithRelated.class)
            .map(productWithRelated -> new Product(productWithRelated.getId(),
                    mappedProducts.get(productWithRelated.getId()).getName(), productWithRelated.getProducts()))
            //.flatMap(this::fetchStockOfRelated)
            .collectList()
            .flatMapMany(this::fetchStockOfRelated)
            ;
  }

  private Mono<Product> fetchStockOfRelated(Product product) {
    if (CollectionUtils.isEmpty(product.getRelated())) {
      return Mono.just(product);
    }
    Map<String, Product> mappedProducts = product.getRelated().stream().collect(Collectors.toMap(Product::getId, Function.identity()));
    return stockWebClient.get()
            .uri(uriBuilder -> uriBuilder
                    .path("/products/stocks")
                    .queryParam("ids", product.getRelated().stream().map(Product::getId).collect(toSet()))
                    .build()
            )
            .retrieve()
            .bodyToFlux(ProductWithStock.class)
            .filter(productWithStock -> productWithStock.getStock() > 0)
            .map(productWithStock -> new Product(productWithStock.getId(),
                    mappedProducts.get(productWithStock.getId()).getName(), null))
            .collectList()
            .map(related -> new Product(product.getId(), product.getName(), related));
  }

  private Flux<Product> fetchStockOfRelated(List<Product> products) {
    Set<String> ids = products.stream().map(Product::getRelated).flatMap(Collection::stream).map(Product::getId).collect(Collectors.toSet());

    return stockWebClient.get()
            .uri(uriBuilder -> uriBuilder
                    .path("/products/stocks")
                    .queryParam("ids", ids)
                    .build()
            )
            .retrieve()
            .bodyToFlux(ProductWithStock.class)
            .filter(productWithStock -> productWithStock.getStock() > 0)
            .collectList()
            .map(list -> list.stream().filter(productWithStock -> productWithStock.getStock() > 0)
                    .collect(Collectors.toMap(ProductWithStock::getId, ProductWithStock::getStock)))
            .flatMapIterable(map -> products.stream()
                    .map(product -> new Product(product.getId(), product.getName(),
                            product.getRelated().stream().filter(related -> map.containsKey(related.getId())).collect(Collectors.toList())))
                    .collect(Collectors.toList()))
            ;
  }

  @Data
  private static class ProductWithRelated {
    private String id;
    private List<Product> products;
  }

  @Data
  private static class ProductWithStock {
    private String id;
    private Integer stock;
  }
}
