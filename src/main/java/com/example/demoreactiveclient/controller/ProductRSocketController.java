package com.example.demoreactiveclient.controller;

import io.rsocket.core.Resume;
import io.rsocket.frame.decoder.PayloadDecoder;
import lombok.Data;
import org.springframework.http.MediaType;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.rsocket.frame.decoder.PayloadDecoder.*;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

@RestController
@RequestMapping("/rsocket")
public class ProductRSocketController {

  private final RSocketRequester rSocketRequester;

  static final Resume resume =
          new Resume()
                  .sessionDuration(Duration.ofMinutes(15))
                  .retry(
                          Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(1)));

  public ProductRSocketController(RSocketRequester.Builder builder) {
    rSocketRequester = builder
            .rsocketConnector(connector -> connector.resume(resume)
                    .reconnect(Retry.fixedDelay(Integer.MAX_VALUE, Duration.ofSeconds(1)))
                    .payloadDecoder(ZERO_COPY)
            )
            .tcp("localhost", 8000)
    ;
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
            .doOnComplete(() -> System.out.println("Rsocket : " + (System.currentTimeMillis() - start)));
  }

  private Flux<Product> getProductFlux() {
    return rSocketRequester.route("products.all")
            .retrieveFlux(Product.class);
  }

  // V2
  private Flux<ProductWithRelated> fetchRelated(Flux<Product> products) {
    return rSocketRequester.route("products.related.batch")
            .data(products.map(Product::getId))
            .retrieveFlux(ProductWithRelated.class)
            ;
  }

  private Flux<Product> filterAvailable(Flux<ProductWithRelated> products) {
    return fetchProductsByIds(filterPositiveStocksByIds(products
            .flatMapIterable(product -> product.getProducts())));
  }

  private Flux<Product> fetchProductsByIds(Flux<String> ids) {
    return rSocketRequester.route("products.batch.stream")
            .data(ids)
            .retrieveFlux(Product.class);
  }

  private Flux<String> filterPositiveStocksByIds(Flux<String> ids) {
    return rSocketRequester.route("products.stock.batch")
            .data(ids)
            .retrieveFlux(ProductWithStock.class)
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
