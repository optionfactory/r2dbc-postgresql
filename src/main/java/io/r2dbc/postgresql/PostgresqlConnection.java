/*
 * Copyright 2017 the original author or authors.
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

import io.r2dbc.postgresql.api.CopyInBuilder;
import io.r2dbc.postgresql.api.ErrorDetails;
import io.r2dbc.postgresql.api.Notice;
import io.r2dbc.postgresql.api.Notification;
import io.r2dbc.postgresql.api.PostgresTransactionDefinition;
import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.postgresql.api.PostgresqlStatement;
import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.ConnectionContext;
import io.r2dbc.postgresql.client.PortalNameSupplier;
import io.r2dbc.postgresql.client.SimpleQueryMessageFlow;
import io.r2dbc.postgresql.client.TransactionStatus;
import io.r2dbc.postgresql.codec.Codecs;
import io.r2dbc.postgresql.message.backend.BackendMessage;
import io.r2dbc.postgresql.message.backend.CommandComplete;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.message.backend.Field;
import io.r2dbc.postgresql.message.backend.NoticeResponse;
import io.r2dbc.postgresql.message.backend.NotificationResponse;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.Operators;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Option;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static io.r2dbc.postgresql.client.TransactionStatus.IDLE;
import static io.r2dbc.postgresql.client.TransactionStatus.OPEN;
import org.reactivestreams.Subscriber;

/**
 * An implementation of {@link Connection} for connecting to a PostgreSQL database.
 */
final class PostgresqlConnection implements io.r2dbc.postgresql.api.PostgresqlConnection {

    private final Logger logger = Loggers.getLogger(this.getClass());

    private final Client client;

    private final ConnectionResources resources;

    private final ConnectionContext connectionContext;

    private final Codecs codecs;

    private final Flux<Long> validationQuery;

    private final AtomicReference<NotificationAdapter> notificationAdapter = new AtomicReference<>();

    private final AtomicReference<NoticeAdapter> noticeAdapter = new AtomicReference<>();

    private volatile IsolationLevel isolationLevel;

    private volatile IsolationLevel previousIsolationLevel;

    PostgresqlConnection(Client client, Codecs codecs, PortalNameSupplier portalNameSupplier, StatementCache statementCache, IsolationLevel isolationLevel,
                         PostgresqlConnectionConfiguration configuration) {
        this.client = Assert.requireNonNull(client, "client must not be null");
        this.resources = new ConnectionResources(client, codecs, this, configuration, portalNameSupplier, statementCache);
        this.connectionContext = client.getContext();
        this.codecs = Assert.requireNonNull(codecs, "codecs must not be null");
        this.isolationLevel = Assert.requireNonNull(isolationLevel, "isolationLevel must not be null");
        this.validationQuery = new io.r2dbc.postgresql.PostgresqlStatement(this.resources, "SELECT 1").fetchSize(0).execute().flatMap(PostgresqlResult::getRowsUpdated);
    }

    Client getClient() {
        return this.client;
    }

    ConnectionResources getResources() {
        return this.resources;
    }

    @Override
    public Mono<Void> beginTransaction() {
        return beginTransaction(EmptyTransactionDefinition.INSTANCE);
    }

    @Override
    public Mono<Void> beginTransaction(TransactionDefinition definition) {
        Assert.requireNonNull(definition, "definition must not be null");

        return useTransactionStatus(transactionStatus -> {
            if (IDLE == transactionStatus) {

                IsolationLevel isolationLevel = definition.getAttribute(TransactionDefinition.ISOLATION_LEVEL);
                Boolean readOnly = definition.getAttribute(TransactionDefinition.READ_ONLY);
                Boolean deferrable = definition.getAttribute(PostgresTransactionDefinition.DEFERRABLE);

                String begin = "BEGIN";
                String transactionMode = "";

                if (isolationLevel != null) {
                    transactionMode = appendTransactionMode(transactionMode, "ISOLATION LEVEL", isolationLevel.asSql());
                }

                if (readOnly != null) {
                    transactionMode = appendTransactionMode(transactionMode, readOnly ? "READ ONLY" : "READ WRITE");
                }

                if (deferrable != null) {
                    transactionMode = appendTransactionMode(transactionMode, deferrable ? "" : "NOT", "DEFERRABLE");
                }

                return exchange(transactionMode.isEmpty() ? begin : (begin + " " + transactionMode)).doOnComplete(() -> {

                    this.previousIsolationLevel = this.isolationLevel;

                    if (isolationLevel != null) {
                        this.isolationLevel = isolationLevel;
                    }
                });
            } else {
                this.logger.debug(this.connectionContext.getMessage("Skipping begin transaction because status is {}"), transactionStatus);
                return Mono.empty();
            }
        });
    }

    private static String appendTransactionMode(String transactionMode, String... tokens) {

        StringBuilder builder = new StringBuilder(transactionMode);

        boolean first = true;
        if (builder.length() != 0) {
            builder.append(", ");
        }

        for (String token : tokens) {

            if (token.isEmpty()) {
                continue;
            }

            if (first) {
                first = false;
            } else {
                builder.append(" ");
            }
            builder.append(token);
        }

        return builder.toString();
    }

    @Override
    public Mono<Void> close() {
        return this.client.close().doOnSubscribe(subscription -> {

            NotificationAdapter notificationAdapter = this.notificationAdapter.get();

            if (notificationAdapter != null && this.notificationAdapter.compareAndSet(notificationAdapter, null)) {
                notificationAdapter.dispose();
            }

            NoticeAdapter noticeAdapter = this.noticeAdapter.get();

            if (noticeAdapter != null && this.noticeAdapter.compareAndSet(noticeAdapter, null)) {
                noticeAdapter.dispose();
            }
        }).then(Mono.empty());
    }

    @Override
    public Mono<Void> cancelRequest() {
        return this.client.cancelRequest();
    }

    @Override
    public Mono<Void> commitTransaction() {

        AtomicReference<R2dbcException> ref = new AtomicReference<>();
        return useTransactionStatus(transactionStatus -> {
            if (IDLE != transactionStatus) {
                return Flux.from(exchange("COMMIT"))
                    .doOnComplete(this::cleanupIsolationLevel)
                    .filter(CommandComplete.class::isInstance)
                    .cast(CommandComplete.class)
                    .<BackendMessage>handle((message, sink) -> {

                        // Certain backend versions (e.g. 12.2, 11.7, 10.12, 9.6.17, 9.5.21, etc)
                        // silently rollback the transaction in the response to COMMIT statement
                        // in case the transaction has failed.
                        // See discussion in pgsql-hackers: https://www.postgresql.org/message-id/b9fb50dc-0f6e-15fb-6555-8ddb86f4aa71%40postgresfriends.org

                        if ("ROLLBACK".equalsIgnoreCase(message.getCommand())) {
                            ErrorDetails details = ErrorDetails.fromMessage("The database returned ROLLBACK, so the transaction cannot be committed. Transaction " +
                                "failure is not known (check server logs?)");
                            ref.set(new ExceptionFactory.PostgresqlRollbackException(details, "COMMIT"));
                            return;
                        }

                        sink.next(message);
                    }).doOnComplete(() -> {
                        if (ref.get() != null) {
                            throw ref.get();
                        }
                    });
            } else {
                this.logger.debug(this.connectionContext.getMessage("Skipping commit transaction because status is {}"), transactionStatus);
                return Mono.empty();
            }
        });
    }

    @Override
    public CopyInBuilder copyIn(String sql) {
        return new PostgresqlCopyIn.Builder(this.resources, sql);
    }

    @Override
    public PostgresqlBatch createBatch() {
        return new PostgresqlBatch(this.resources);
    }

    @Override
    public Mono<Void> createSavepoint(String name) {
        Assert.requireNonNull(name, "name must not be null");

        return beginTransaction()
            .then(useTransactionStatus(transactionStatus -> {
                if (OPEN == transactionStatus) {
                    return exchange(String.format("SAVEPOINT %s", name));
                } else {
                    this.logger.debug(this.connectionContext.getMessage("Skipping create savepoint because status is {}"), transactionStatus);
                    return Mono.empty();
                }
            }));
    }

    @Override
    public PostgresqlStatement createStatement(String sql) {
        Assert.requireNonNull(sql, "sql must not be null");
        return new io.r2dbc.postgresql.PostgresqlStatement(this.resources, sql);
    }

    /**
     * Return a {@link Flux} of {@link Notification} received from {@code LISTEN} registrations.
     * The stream is a hot stream producing messages as they are received.
     *
     * @return a hot {@link Flux} of {@link Notification Notifications}.
     */
    @Override
    public Flux<Notification> getNotifications() {

        NotificationAdapter notifications = this.notificationAdapter.get();

        if (notifications == null) {

            notifications = new NotificationAdapter();

            if (this.notificationAdapter.compareAndSet(null, notifications)) {
                notifications.register(this.client);
            } else {
                notifications = this.notificationAdapter.get();
            }
        }

        return notifications.getEvents();
    }

    @Override
    public Flux<Notice> getNotices() {
        NoticeAdapter notices = this.noticeAdapter.get();

        if (notices == null) {

            notices = new NoticeAdapter();

            if (this.noticeAdapter.compareAndSet(null, notices)) {
                notices.register(this.client);
            } else {
                notices = this.noticeAdapter.get();
            }
        }

        return notices.getEvents();
    }

    @Override
    public PostgresqlConnectionMetadata getMetadata() {
        return new PostgresqlConnectionMetadata(this.client.getVersion());
    }

    @Override
    public IsolationLevel getTransactionIsolationLevel() {
        return this.isolationLevel;
    }

    @Override
    public boolean isAutoCommit() {

        if (this.client.getTransactionStatus() == IDLE) {
            return true;
        }

        return false;
    }

    @Override
    public Mono<Void> releaseSavepoint(String name) {
        Assert.requireNonNull(name, "name must not be null");

        return useTransactionStatus(transactionStatus -> {
            if (OPEN == transactionStatus) {
                return exchange(String.format("RELEASE SAVEPOINT %s", name));
            } else {
                this.logger.debug(this.connectionContext.getMessage("Skipping release savepoint because status is {}"), transactionStatus);
                return Mono.empty();
            }
        });
    }

    @Override
    public Mono<Void> rollbackTransaction() {
        return useTransactionStatus(transactionStatus -> {
            if (IDLE != transactionStatus) {
                return exchange("ROLLBACK").doOnComplete(this::cleanupIsolationLevel);
            } else {
                this.logger.debug(this.connectionContext.getMessage("Skipping rollback transaction because status is {}"), transactionStatus);
                return Mono.empty();
            }
        });
    }

    @Override
    public Mono<Void> rollbackTransactionToSavepoint(String name) {
        Assert.requireNonNull(name, "name must not be null");

        return useTransactionStatus(transactionStatus -> {
            if (IDLE != transactionStatus) {
                return exchange(String.format("ROLLBACK TO SAVEPOINT %s", name));
            } else {
                this.logger.debug(this.connectionContext.getMessage("Skipping rollback transaction to savepoint because status is {}"), transactionStatus);
                return Mono.empty();
            }
        });
    }

    @Override
    public Mono<Void> setAutoCommit(boolean autoCommit) {

        return useTransactionStatus(transactionStatus -> {

            this.logger.debug(this.connectionContext.getMessage(String.format("Setting auto-commit mode to [%s]", autoCommit)));

            if (isAutoCommit()) {
                if (!autoCommit) {
                    this.logger.debug(this.connectionContext.getMessage("Beginning transaction"));
                    return beginTransaction();
                }
            } else {

                if (autoCommit) {
                    this.logger.debug(this.connectionContext.getMessage("Committing pending transactions"));
                    return commitTransaction();
                }
            }

            return Mono.empty();
        });
    }

    @Override
    public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        Assert.requireNonNull(isolationLevel, "isolationLevel must not be null");

        return withTransactionStatus(getTransactionIsolationLevelQuery(isolationLevel))
            .flatMapMany(this::exchange)
            .then()
            .doOnSuccess(ignore -> this.isolationLevel = isolationLevel);
    }

    @Override
    public String toString() {
        return "PostgresqlConnection{" +
            "client=" + this.client +
            ", codecs=" + this.codecs +
            '}';
    }

    @Override
    public Mono<Boolean> validate(ValidationDepth depth) {

        if (depth == ValidationDepth.LOCAL) {
            return Mono.fromSupplier(this.client::isConnected);
        }

        return Mono.create(sink -> {

            if (!this.client.isConnected()) {
                sink.success(false);
                return;
            }

            this.validationQuery.subscribe(new CoreSubscriber<Long>() {

                @Override
                public void onSubscribe(Subscription s) {
                    s.request(Integer.MAX_VALUE);
                }

                @Override
                public void onNext(Long integer) {

                }

                @Override
                public void onError(Throwable t) {
                    PostgresqlConnection.this.logger.debug(PostgresqlConnection.this.connectionContext.getMessage("Validation failed"), t);
                    sink.success(false);
                }

                @Override
                public void onComplete() {
                    sink.success(true);
                }
            });
        });
    }

    private static Function<TransactionStatus, String> getTransactionIsolationLevelQuery(IsolationLevel isolationLevel) {
        return transactionStatus -> {
            if (transactionStatus == OPEN) {
                return String.format("SET TRANSACTION ISOLATION LEVEL %s", isolationLevel.asSql());
            } else {
                return String.format("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL %s", isolationLevel.asSql());
            }
        };
    }

    @Override
    public Mono<Void> setLockWaitTimeout(Duration lockTimeout) {
        Assert.requireNonNull(lockTimeout, "lockTimeout must not be null");

        return Mono.defer(() -> Mono.from(exchange(String.format("SET LOCK_TIMEOUT = %s", lockTimeout.toMillis()))).then());
    }

    @Override
    public Mono<Void> setStatementTimeout(Duration statementTimeout) {
        Assert.requireNonNull(statementTimeout, "statementTimeout must not be null");

        return Mono.defer(() -> Mono.from(exchange(String.format("SET STATEMENT_TIMEOUT = %s", statementTimeout.toMillis()))).then());
    }

    private Mono<Void> useTransactionStatus(Function<TransactionStatus, Publisher<?>> f) {
        return Flux.defer(() -> f.apply(this.client.getTransactionStatus()))
            .as(Operators::discardOnCancel)
            .then();
    }

    private <T> Mono<T> withTransactionStatus(Function<TransactionStatus, T> f) {
        return Mono.defer(() -> Mono.just(f.apply(this.client.getTransactionStatus())));
    }

    @SuppressWarnings("unchecked")
    private <T> Flux<T> exchange(String sql) {
        AtomicReference<R2dbcException> ref = new AtomicReference<>();
        return (Flux<T>) SimpleQueryMessageFlow.exchange(this.client, sql)
            .handle((backendMessage, synchronousSink) -> {

                if (backendMessage instanceof ErrorResponse) {
                    ref.set(ExceptionFactory.createException((ErrorResponse) backendMessage, sql));
                } else {
                    synchronousSink.next(backendMessage);
                }
            })
            .doOnComplete(() -> {
                if (ref.get() != null) {
                    throw ref.get();
                }
            });
    }

    private void cleanupIsolationLevel() {
        if (this.previousIsolationLevel != null) {
            this.isolationLevel = this.previousIsolationLevel;
        }

        this.previousIsolationLevel = null;
    }

    /**
     * Generic adapter that maps {@link BackendMessage}s received by subscribing
     * to a {@link Client}
     * @param <T> the exposed message type
     * @param <M> the source message type
     */
    static abstract class BackendMessageAdapter<T, M extends BackendMessage> {

        private final Sinks.Many<T> sink = Sinks.many().multicast().directBestEffort();

        @Nullable
        private volatile Disposable subscription = null;

        void dispose() {
            Disposable subscription = this.subscription;
            if (subscription != null && !subscription.isDisposed()) {
                subscription.dispose();
            }
        }

        abstract T mapMessage(M message);

        abstract void registerSubscriber(Client client, Subscriber<M> subscriber);

        void register(Client client) {

            BaseSubscriber<M> subscriber = new BaseSubscriber<M>() {

                @Override
                protected void hookOnSubscribe(Subscription subscription) {
                    subscription.request(Long.MAX_VALUE);
                }

                @Override
                public void hookOnNext(M notificationResponse) {
                    BackendMessageAdapter.this.sink.emitNext(mapMessage(notificationResponse), Sinks.EmitFailureHandler.FAIL_FAST);
                }

                @Override
                public void hookOnError(Throwable throwable) {
                    BackendMessageAdapter.this.sink.emitError(throwable, Sinks.EmitFailureHandler.FAIL_FAST);
                }

                @Override
                public void hookOnComplete() {
                    BackendMessageAdapter.this.sink.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
                }
            };

            this.subscription = subscriber;
            registerSubscriber(client, subscriber);
        }

        Flux<T> getEvents() {
            return this.sink.asFlux();
        }
    }

    /**
     * Adapter to publish {@link Notification}s.
     */
    static class NotificationAdapter extends BackendMessageAdapter<Notification, NotificationResponse> {

        @Override
        Notification mapMessage(NotificationResponse message) {
            return new NotificationResponseWrapper(message);
        }

        @Override
        void registerSubscriber(Client client, Subscriber<NotificationResponse> subscriber) {
            client.addNotificationListener(subscriber);
        }
    }

    /**
     * Adapter to publish {@link Notice}s.
     */
    static class NoticeAdapter extends BackendMessageAdapter<Notice, NoticeResponse> {

        @Override
        Notice mapMessage(NoticeResponse message) {
            final Notice notice = new Notice(new EnumMap<>(Field.FieldType.class));
            for (Field field : message.getFields()) {
                notice.fields.put(field.getType(), field.getValue());
            }
            return notice;
        }

        @Override
        void registerSubscriber(Client client, Subscriber<NoticeResponse> subscriber) {
            client.addNoticeListener(subscriber);
        }
    }

    enum EmptyTransactionDefinition implements TransactionDefinition {

        INSTANCE;

        @Override
        public <T> T getAttribute(Option<T> option) {
            return null;
        }
    }

}
