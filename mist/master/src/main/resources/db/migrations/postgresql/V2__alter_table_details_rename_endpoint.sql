
--
-- Name: job_details; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.job_details (
    path text NOT NULL,
    class_name text NOT NULL,
    namespace text NOT NULL,
    parameters text NOT NULL,
    external_id text,
    function text,
    action text NOT NULL,
    source text NOT NULL,
    job_id character varying(36) NOT NULL,
    start_time bigint,
    end_time bigint,
    job_result text,
    status text NOT NULL,
    worker_id text,
    create_time bigint NOT NULL
);

ALTER TABLE ONLY public.job_details
    ADD CONSTRAINT job_details_pkey PRIMARY KEY (job_id);

