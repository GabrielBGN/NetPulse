--
-- PostgreSQL database dump
--

\restrict fe0vrAieHXohhQmwQ4ooF9xqUjFfEVfoR2oMmZ5jHo4XgsvfhSa2qxZay4u0iSL

-- Dumped from database version 17.12 (Homebrew)
-- Dumped by pg_dump version 17.12 (Homebrew)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: anomalies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.anomalies (
    anomaly_id bigint NOT NULL,
    telemetry_id bigint NOT NULL,
    anomaly_type character varying(50) NOT NULL,
    detected_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: anomalies_anomaly_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.anomalies_anomaly_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: anomalies_anomaly_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.anomalies_anomaly_id_seq OWNED BY public.anomalies.anomaly_id;


--
-- Name: devices; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.devices (
    device_id character varying(50) NOT NULL,
    device_type character varying(20) NOT NULL,
    ip_address inet NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: telemetry; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.telemetry (
    telemetry_id bigint NOT NULL,
    device_id character varying(50) NOT NULL,
    "timestamp" timestamp with time zone NOT NULL,
    latency_ms double precision,
    packet_loss_pct double precision,
    bandwidth_mbps double precision,
    cpu_usage_pct double precision,
    memory_usage_pct double precision,
    status character varying(10) NOT NULL,
    CONSTRAINT valid_status CHECK (((status)::text = ANY ((ARRAY['online'::character varying, 'offline'::character varying])::text[])))
);


--
-- Name: telemetry_telemetry_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.telemetry_telemetry_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: telemetry_telemetry_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.telemetry_telemetry_id_seq OWNED BY public.telemetry.telemetry_id;


--
-- Name: anomalies anomaly_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.anomalies ALTER COLUMN anomaly_id SET DEFAULT nextval('public.anomalies_anomaly_id_seq'::regclass);


--
-- Name: telemetry telemetry_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.telemetry ALTER COLUMN telemetry_id SET DEFAULT nextval('public.telemetry_telemetry_id_seq'::regclass);


--
-- Name: anomalies anomalies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.anomalies
    ADD CONSTRAINT anomalies_pkey PRIMARY KEY (anomaly_id);


--
-- Name: devices devices_ip_address_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.devices
    ADD CONSTRAINT devices_ip_address_key UNIQUE (ip_address);


--
-- Name: devices devices_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.devices
    ADD CONSTRAINT devices_pkey PRIMARY KEY (device_id);


--
-- Name: telemetry telemetry_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.telemetry
    ADD CONSTRAINT telemetry_pkey PRIMARY KEY (telemetry_id);


--
-- Name: telemetry fk_device; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.telemetry
    ADD CONSTRAINT fk_device FOREIGN KEY (device_id) REFERENCES public.devices(device_id);


--
-- Name: anomalies fk_telemetry; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.anomalies
    ADD CONSTRAINT fk_telemetry FOREIGN KEY (telemetry_id) REFERENCES public.telemetry(telemetry_id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict fe0vrAieHXohhQmwQ4ooF9xqUjFfEVfoR2oMmZ5jHo4XgsvfhSa2qxZay4u0iSL

