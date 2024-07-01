package net.samitkumar.voting_result_api;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.repository.ListCrudRepository;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootApplication
public class VotingResultApiApplication {


    public static void main(String[] args) {
        SpringApplication.run(VotingResultApiApplication.class, args);
    }

    @Bean
    Sinks.Many<String> sinks() {
        return Sinks.many().multicast().onBackpressureBuffer();
    }

	@Bean
	RouterFunction<ServerResponse> appRouter(VoteOptionRepository voteOptionRepository) {
		return RouterFunctions
				.route()
				.path("/vote-options", builder -> builder
						.GET("", request -> ServerResponse
								.ok()
								.bodyValue(voteOptionRepository.findAll())
						)
						.POST("", request -> request
								.bodyToMono(VoteOption.class)
								.map(voteOptionRepository::save)
								.flatMap(ServerResponse.ok()::bodyValue)
						)
				)
				.build();
	}
}

@Configuration
@EnableScheduling
@RequiredArgsConstructor
@Slf4j
class SchedulerConfiguration {
    final Sinks.Many<String> sinks;
    final VotesRepository votesRepository;
    final ObjectMapper objectMapper;

    @Scheduled(fixedRate = 10000)
    void scheduled() {
        log.info("Scheduled trigger to inform all ws session");
        sinks.tryEmitNext(emitResponse());
    }

    @SneakyThrows
    private String emitResponse() {
        var votingResult = new VotingResult(
                LocalDateTime.now(),
                prepareResult(votesRepository.findVoteCountsByOption()));
        return objectMapper
                .writeValueAsString(votingResult);
    }

    private List<Result> prepareResult(List<VoteOptionCount> voteOptionCounts) {
        return voteOptionCounts
                .stream()
                .map(voteCount -> new Result(voteCount.option(), voteCount.count()))
                .toList();
    }
}

@Table("votes")
record Votes(
        @Id Long id,
        @Column("option_id") String optionId,
        @Column("user_id") String userId,
        @Column("created_at") LocalDateTime createdAt) {
}

record VoteOptionCount(String option, int count) {
}

interface VotesRepository extends ListCrudRepository<Votes, Long> {
    @Query("""
            		SELECT vo.option AS option, COUNT(v.option_id) AS count
            		FROM votes v
            		JOIN vote_options vo ON v.option_id = vo.id
            		GROUP BY vo.option
            """)
    List<VoteOptionCount> findVoteCountsByOption();
}

@Table("vote_options")
record VoteOption(@Id Long id, String option) {
}

interface VoteOptionRepository extends ListCrudRepository<VoteOption, String> {
}


record Result(String name, Integer count) {
}

record VotingResult(LocalDateTime localDateTime, List<Result> results) {
}

@Configuration
@RequiredArgsConstructor
class WebConfig {
    final Sinks.Many<String> sinks;

    @Bean
    public HandlerMapping handlerMapping() {
        Map<String, WebSocketHandler> map = new HashMap<>();
        map.put("/ws", session -> session
                .send(sinks.asFlux().map(session::textMessage))
                //echo back , if there is a message
                .and(session
                        .receive()
                        .map(webSocketMessage -> sinks.tryEmitNext(webSocketMessage.getPayloadAsText()))
                )
                .then());
        int order = -1;
        return new SimpleUrlHandlerMapping(map, order);
    }
}
