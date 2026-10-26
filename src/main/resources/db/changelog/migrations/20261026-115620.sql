--liquibase formatted sql

--changeset migrate-generate:20261026-115620
ALTER TABLE public.vehicles ADD source_id BIGINT;
CREATE UNIQUE INDEX ux_vehicles_source_id ON public.vehicles USING btree (source_id);
--rollback DROP INDEX public.ux_vehicles_source_id;
--rollback ALTER TABLE public.vehicles DROP COLUMN source_id;
