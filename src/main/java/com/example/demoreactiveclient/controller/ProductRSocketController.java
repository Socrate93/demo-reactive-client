package com.example.demoreactiveclient.controller;

import io.rsocket.core.Resume;
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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

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
            )
            .tcp("localhost", 8000)
    ;
  }

  @GetMapping(value = "/products", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public Flux<Product> getProducts() {
    long start = System.currentTimeMillis();
    return getProductFlux()
            .flatMap(this::fetchRelated)
            //.collectList()
            //.flatMapMany(this::fetchRelated)
            .doOnComplete(() -> System.out.println("Rsocket : " + (System.currentTimeMillis() - start)));
  }

  private Flux<Product> getProductFlux() {
    return rSocketRequester.route("products.all")
            .retrieveFlux(Product.class);
  }

  private Mono<Product> fetchRelated(Product product) {
    return rSocketRequester.route("products.related")
            .data(product.getId())
            .retrieveFlux(Product.class)
            .filterWhen(this::fetchStock)
            .collectList()
            .map(related -> new Product(product.getId(), product.getName(), related));
  }

  private Mono<Boolean> fetchStock(Product product) {
    return rSocketRequester.route("products.stock")
            .data(product.getId())
            .retrieveMono(Integer.class)
            .map(stock -> stock > 0);
  }

  // V2
  private Flux<Product> fetchRelated(List<Product> products) {
    Map<String, Product> mappedProducts = products.stream().collect(Collectors.toMap(Product::getId, Function.identity()));
    return rSocketRequester.route("products.related.batch")
            .data(products.stream().map(Product::getId).collect(toSet()))
            .retrieveFlux(ProductWithRelated.class)
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
    Set<String> ids = product.getRelated().stream().map(Product::getId).collect(toSet());
    return rSocketRequester.route("products.stock.batch")
            .data(ids)
            .retrieveFlux(ProductWithStock.class)
            .filter(productWithStock -> productWithStock.getStock() > 0)
            .map(productWithStock -> new Product(productWithStock.getId(),
                    mappedProducts.get(productWithStock.getId()).getName(), null))
            .collectList()
            .map(related -> new Product(product.getId(), product.getName(), related));
  }

  private Flux<Product> fetchStockOfRelated(List<Product> products) {
    Set<String> ids = products.stream()
            .map(Product::getRelated)
            .flatMap(Collection::stream)
            .map(Product::getId)
            .collect(Collectors.toSet());
    return rSocketRequester.route("products.stock.batch")
            .data(ids)
            .retrieveFlux(ProductWithStock.class)
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
