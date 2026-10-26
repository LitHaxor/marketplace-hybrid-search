--liquibase formatted sql

--changeset migrate-generate:20261026-010053
ALTER TABLE public.outbox ADD topic TEXT;
ALTER TABLE public.vehicles ADD status TEXT;
ALTER TABLE public.vehicles ADD version BIGINT;
--rollback ALTER TABLE public.vehicles DROP COLUMN status;
--rollback ALTER TABLE public.vehicles DROP COLUMN version;
--rollback ALTER TABLE public.outbox DROP COLUMN topic;
