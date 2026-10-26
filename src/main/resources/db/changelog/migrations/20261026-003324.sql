--liquibase formatted sql

--changeset migrate-generate:20261026-003324
CREATE TABLE public.outbox (id UUID NOT NULL, aggregate_type TEXT, aggregate_id TEXT, type TEXT, payload TEXT, created_at TIMESTAMP WITH TIME ZONE, CONSTRAINT outbox_pkey PRIMARY KEY (id));
--rollback DROP TABLE public.outbox;
