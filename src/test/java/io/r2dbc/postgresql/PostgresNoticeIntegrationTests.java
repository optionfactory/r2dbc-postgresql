/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql;

import io.netty.channel.Channel;
import io.r2dbc.postgresql.api.Notice;
import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.postgresql.message.backend.Field;
import io.r2dbc.postgresql.util.ConnectionIntrospector;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.test.StepVerifier;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link Notice} through {@link PostgresqlConnection#getNotices()}.
 */
final class PostgresNoticeIntegrationTests extends AbstractIntegrationTests {

    private static final String RAISE_INFO_FUNCTION =
            "CREATE OR REPLACE FUNCTION raise_info(text)"
                + " RETURNS void AS $$"
                + " BEGIN"
                + "   RAISE INFO '%', $1;"
                + " END;"
                + " $$ LANGUAGE plpgsql;";

    @Test
    void shouldReceivePubSubNotices() throws Exception {

        BlockingQueue<Notice> notices = new LinkedBlockingQueue<>();

        this.connectionFactory.create().flatMap(it -> {
            it.getNotices().doOnNext(notices::add).subscribe();
            return it.createStatement(RAISE_INFO_FUNCTION).execute().then()
                    .then(it.createStatement("SELECT raise_info('Test Message')").execute().then());
        }).block(Duration.ofSeconds(10));

        Notice notice = notices.poll(10, TimeUnit.SECONDS);

        assertThat(notice).isNotNull();
        assertThat(notice.fields.containsKey(Field.FieldType.MESSAGE)).isTrue();
        assertThat(notice.fields.get(Field.FieldType.MESSAGE)).isEqualTo("Test Message");
    }

    @Test
    void listenShouldCompleteOnConnectionClose() {

        PostgresqlConnection connection = this.connectionFactory.create().block();

        connection.getNotices().as(StepVerifier::create).expectSubscription()
            .then(() -> connection.close().subscribe())
            .verifyComplete();
    }

    @Test
    void listenShouldFailOnConnectionDisconnected() {

        PostgresqlConnection connection = this.connectionFactory.create().block();

        connection.getNotices().as(StepVerifier::create).expectSubscription()
            .then(() -> {
                Channel channel = ConnectionIntrospector.of(connection).getChannel();
                channel.close();
            })
            .verifyError(R2dbcNonTransientResourceException.class);
    }
}
