package com.example.demoreactiveclient.controller;

import lombok.Data;
import org.reactivestreams.Publisher;
import org.springframework.core.ParameterizedTypeReference;
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

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
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
            .baseUrl("http://192.168.1.29:9000")
            .build();
    this.stockWebClient = WebClient.builder()
            .clientConnector(new ReactorClientHttpConnector(nettyHttpClient))
            .baseUrl("http://192.168.1.29:9000")
            .build();
    this.relatedWebClient = WebClient.builder()
            .clientConnector(new ReactorClientHttpConnector(nettyHttpClient))
            .baseUrl("http://192.168.1.29:9000")
            .build();
  }

  @GetMapping(value = "/products", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public Flux<Product> getProducts() {
    long start = System.currentTimeMillis();
    Flux<Product> productsFlux = getProductFlux().cache();
    Flux<ProductWithRelated> productsWithRelatedFlux = fetchRelated(productsFlux).cache();
    return Mono.zip(productsFlux.collectList(), productsWithRelatedFlux.collectList(),
            filterAvailable(productsWithRelatedFlux).collectList())
            .map(tuple -> {
              Map<String, List<String>> mappedRelated = tuple.getT2()
                      .stream()
                      .collect(toMap(ProductWithRelated::getId, ProductWithRelated::getProducts));
              Map<String, Product> mappedProducts = tuple.getT3()
                      .stream()
                      .collect(toMap(Product::getId, identity(), (p1, p2) -> p1));
              return tuple.getT1()
                      .stream()
                      .map(product -> new Product(product.getId(), product.getName(),
                              mappedRelated.getOrDefault(product.getId(), List.of())
                                      .stream()
                                      .map(mappedProducts::get)
                                      .collect(Collectors.toList())
                      ))
                      .collect(Collectors.toList());
            })
            .flatMapIterable(identity())
            .doOnComplete(() -> System.out.println("Reactive HTTP : " + (System.currentTimeMillis() - start)));
  }

  private Flux<Product> getProductFlux() {
    return webClient.get()
            .uri("/products")
            .retrieve()
            .bodyToFlux(Product.class);
  }

  // V2
  private Flux<ProductWithRelated> fetchRelated(Flux<Product> products) {
    return relatedWebClient.post()
            .uri("/products/related/searches")
            .body(products.map(Product::getId).collectList(), new ParameterizedTypeReference<Object>() {})
            .retrieve()
            .bodyToFlux(ProductWithRelated.class)
            ;
  }

  private Flux<Product> filterAvailable(Flux<ProductWithRelated> products) {
    return fetchProductsByIds(filterPositiveStocksByIds(products
            .flatMapIterable(product -> product.getProducts())));
  }

  private Flux<Product> fetchProductsByIds(Flux<String> ids) {
    return webClient.post()
            .uri("/products/searches")
            .body(ids.collectList(), new ParameterizedTypeReference<Object>() {})
            .retrieve()
            .bodyToFlux(Product.class);
  }

  private Flux<String> filterPositiveStocksByIds(Flux<String> ids) {
    return stockWebClient.post()
            .uri("/products/stocks/searches")
            .body(ids.collectList(), new ParameterizedTypeReference<Object>() {})
            .retrieve()
            .bodyToFlux(ProductWithStock.class)
            .filter(productWithStock -> productWithStock.getStock() > 0)
            .map(ProductWithStock::getId);
  }

  @Data
  private static class ProductWithRelated {
    private String id;
    private List<String> products;
  }

  @Data
  private static class ProductWithStock {
    private String id;
    private Integer stock;
  }
}
