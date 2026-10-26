--liquibase formatted sql

--changeset migrate-generate:20261025-230906
CREATE TABLE public.vehicles (id UUID NOT NULL, price INTEGER, year INTEGER, manufacturer TEXT, model TEXT, description TEXT, lat DOUBLE PRECISION, lon DOUBLE PRECISION, CONSTRAINT vehicles_pkey PRIMARY KEY (id));
--rollback DROP TABLE public.vehicles;
