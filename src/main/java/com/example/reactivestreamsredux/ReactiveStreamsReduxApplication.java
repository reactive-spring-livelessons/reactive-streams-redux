package com.example.reactivestreamsredux;

import akka.actor.ActorSystem;
import akka.japi.function.Function;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Source;
import akka.stream.scaladsl.Sink;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.reactivestreams.Publisher;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Flux;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;

@SpringBootApplication
public class ReactiveStreamsReduxApplication {

		public static void main(String[] args) {
				SpringApplication.run(ReactiveStreamsReduxApplication.class, args);
		}
}

@Component
class TweetDataInitializer implements ApplicationRunner {

		private final TweetRepository tweetRepository;

		TweetDataInitializer(TweetRepository tweetRepository) {
				this.tweetRepository = tweetRepository;
		}

		@Override
		public void run(ApplicationArguments args) throws Exception {
				Author viktor = new Author("viktorklang"),
					jonas = new Author("jboner"),
					josh = new Author("starbuxman");

				Flux<Tweet> tweetFlux = Flux.just(
					new Tweet("Woot, Konrad will be talking about #Enterprise #Integration done right! #akka #alpakka", viktor),
					new Tweet("#scala implicits can easily be used to model Capabilities, but can they encode Obligations easily?\n\n* Easy as in: ergonomically.", viktor),
					new Tweet("This is so cool! #akka", viktor),
					new Tweet("Cross Data Center replication of Event Sourced #Akka Actors is soon available (using #CRDTs, and more).", jonas),
					new Tweet("a reminder: @SpringBoot lets you pair-program with the #Spring team.", josh),
					new Tweet("whatever your next #platform is, don't build it yourself. \n\nEven companies with the $$ and motivation to do it fail. a LOT.", josh)
				);

				tweetRepository
					.deleteAll()
					.thenMany(tweetRepository.saveAll(tweetFlux))
					.thenMany(tweetRepository.findAll())
					.subscribe(System.out::println);
		}
}

@Configuration
class AkkaConfiguration {

		@Bean
		ActorSystem actorSystem() {
				return ActorSystem.create("bootiful-akka-streams");
		}

		@Bean
		ActorMaterializer actorMaterializer() {
				return ActorMaterializer.create(this.actorSystem());
		}
}

@Service
class TweetService {

		private final TweetRepository tweetRepository;
		private final ActorMaterializer actorMaterializer;

		TweetService(TweetRepository tweetRepository,
															ActorMaterializer actorMaterializer) {
				this.tweetRepository = tweetRepository;
				this.actorMaterializer = actorMaterializer;
		}

		public Publisher<Tweet> getTweets() {
				return this.tweetRepository.findAll();
		}

		public Publisher<HashTag> getHashTags() {
				return Source
					.fromPublisher(getTweets())
					.map(Tweet::getHashTags)
					.reduce(this::join)
					.mapConcat((Function<Set<HashTag>, Iterable<HashTag>>) identity -> identity)
					.runWith(Sink.asPublisher(true), this.actorMaterializer);
		}

		private <T> Set<T> join(Set<T> a,
																										Set<T> b) {
				Set<T> set = new HashSet<>();
				set.addAll(a);
				set.addAll(b);
				return set;
		}
}

@Configuration
class TweetRouteConfiguration {

		private final TweetService tweetService;

		TweetRouteConfiguration(TweetService tweetService) {
				this.tweetService = tweetService;
		}

		@Bean
		RouterFunction<ServerResponse> routes() {
				return route(GET("/tweets"), request -> ServerResponse.ok().body(tweetService.getTweets(), Tweet.class))
					.andRoute(GET("/hashtags/unique"), request -> ServerResponse.ok().body(tweetService.getHashTags(), HashTag.class));
		}
}

interface TweetRepository extends ReactiveMongoRepository<Tweet, String> {
}

@Document
@AllArgsConstructor
@Data
@NoArgsConstructor
class Author {

		@Id
		private String handle;
}


@Document
@AllArgsConstructor
@Data
@NoArgsConstructor
class HashTag {
		@Id
		private String id;
}


@Document
@AllArgsConstructor
@Data
@NoArgsConstructor
class Tweet {

		@Id
		private String id;
		private String text;
		private Author author;

		Tweet(String t, Author a) {
				this.author = a;
				this.text = t;
		}

		public Set<HashTag> getHashTags() {
				return Arrays
					.stream(text.split(" "))
					.filter(t -> t.startsWith("#"))
					.map(w -> new HashTag(
						w.replaceAll("[^#\\w]", "").toLowerCase()
					))
					.collect(Collectors.toSet());
		}
}