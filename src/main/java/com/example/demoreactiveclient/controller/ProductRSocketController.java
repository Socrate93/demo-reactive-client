package com.example.demoreactiveclient.controller;

import io.rsocket.core.Resume;
import lombok.Data;
import org.springframework.http.MediaType;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.rsocket.frame.decoder.PayloadDecoder.*;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

@RestController
@RequestMapping("/rsocket")
public class ProductRSocketController {

  private static int NB_REQUESTERS = 4;
  private static final Random random = new Random();
  private final List<RSocketRequester> requesters;
  private static final Map<Integer, Long> requestersLatency = IntStream.range(0, NB_REQUESTERS)
          .boxed()
          .collect(Collectors.toMap(Function.identity(), i -> System.currentTimeMillis()));
  private final Map<Integer, AtomicInteger> stats = new HashMap<>();

  static final Resume resume =
          new Resume()
                  .sessionDuration(Duration.ofMinutes(15))
                  .retry(
                          Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(1)));
  private final RSocketRequester.Builder builder;

  public ProductRSocketController(RSocketRequester.Builder builder) {
    this.builder = builder;
    requesters = IntStream.range(0, NB_REQUESTERS)
            .mapToObj(i -> builder
                    .rsocketConnector(connector -> connector.resume(resume)
                            .reconnect(Retry.fixedDelay(Integer.MAX_VALUE, Duration.ofSeconds(1)))
                            .payloadDecoder(ZERO_COPY)
                    )
                    .tcp("localhost", 8000))
            .collect(Collectors.toList());
  }

  private RSocketRequester getRequester(int i) {
    stats.computeIfAbsent(i, n -> new AtomicInteger(0));
    stats.get(i).incrementAndGet();
    return requesters.get(i);
  }

  @GetMapping(value = "/products", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public Flux<Product> getProducts() {
    var currentRequesterIndex = getIndex();
    long start = System.currentTimeMillis();
    Flux<Product> productsFlux = getProductFlux(currentRequesterIndex).cache();
    Flux<ProductWithRelated> productsWithRelatedFlux = fetchRelated(productsFlux, currentRequesterIndex).cache();
    return Mono.zip(productsFlux.collectList(), productsWithRelatedFlux.collectList(),
            filterAvailable(productsWithRelatedFlux, currentRequesterIndex).collectList())
            .flatMapMany(tuple -> {
              Map<String, List<String>> mappedRelated = tuple.getT2()
                      .stream()
                      .collect(toMap(ProductWithRelated::getId, ProductWithRelated::getProducts));
              Map<String, Product> mappedProducts = tuple.getT3()
                      .stream()
                      .collect(toMap(Product::getId, identity(), (p1, p2) -> p1));
              return Flux.fromIterable(tuple.getT1())
                      .map(product -> new Product(product.getId(), product.getName(),
                              mappedRelated.getOrDefault(product.getId(), List.of())
                                      .stream()
                                      .map(mappedProducts::get)
                                      .collect(Collectors.toList())
                      ));
            })
            //.log(null, Level.INFO, ON_SUBSCRIBE, ON_COMPLETE)
            .doOnComplete(() -> {
              var time = System.currentTimeMillis() - start;
              System.out.println("{" + currentRequesterIndex + "} Rsocket : " + time + "-- " + stats);
              if (time > 300) {
                skipRequester(currentRequesterIndex);
              }
            });
  }

  private int getIndex() {
    var index = random.nextInt(NB_REQUESTERS);
    var loops = 0;
    while (requestersLatency.get(index) > System.currentTimeMillis()) {
      if (loops < NB_REQUESTERS) {
        index = random.nextInt(NB_REQUESTERS);
      } else {
        NB_REQUESTERS++;
        requesters.add(builder
                .rsocketConnector(connector -> connector.resume(resume)
                        .reconnect(Retry.fixedDelay(Integer.MAX_VALUE, Duration.ofSeconds(1)))
                        .payloadDecoder(ZERO_COPY)
                )
                .tcp("localhost", 8000));
        requestersLatency.put(NB_REQUESTERS - 1, 0L);
        stats.put(NB_REQUESTERS - 1, new AtomicInteger(0));
        return NB_REQUESTERS - 1;
      }
      loops++;
    }
    return index;
  }

  private static void skipRequester(int index) {
    requestersLatency.put(index, System.currentTimeMillis() + 10_000);
  }

  private Flux<Product> getProductFlux(int index) {
    return getRequester(index).route("products.all")
            .retrieveFlux(Product.class)
            //.log(null, Level.INFO, ON_SUBSCRIBE, ON_COMPLETE)
            ;
  }

  // V2
  private Flux<ProductWithRelated> fetchRelated(Flux<Product> products, int index) {
    return getRequester(index).route("products.related.batch")
            .data(products.map(Product::getId))
            .retrieveFlux(ProductWithRelated.class)
            //.log(null, Level.INFO, ON_SUBSCRIBE, ON_COMPLETE)
            ;
  }

  private Flux<Product> filterAvailable(Flux<ProductWithRelated> products, int index) {
    return fetchProductsByIds(filterPositiveStocksByIds(products
            .flatMapIterable(product -> product.getProducts()), index), index);
  }

  private Flux<Product> fetchProductsByIds(Flux<String> ids, int index) {
    return getRequester(index).route("products.batch.stream")
            .data(ids.distinct())
            .retrieveFlux(Product.class)
            //.log(null, Level.INFO, ON_SUBSCRIBE, ON_COMPLETE)
            ;
  }

  private Flux<String> filterPositiveStocksByIds(Flux<String> ids, int index) {
    return getRequester(index).route("products.stock.batch")
            .data(ids.distinct())
            .retrieveFlux(ProductWithStock.class)
            //.log(null, Level.INFO, ON_SUBSCRIBE, ON_COMPLETE)
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
