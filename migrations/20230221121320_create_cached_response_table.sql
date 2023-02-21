CREATE TABLE cached_response (
    key text NOT NULL,
    response text NOT NULL
);

ALTER TABLE ONLY cached_response
    ADD CONSTRAINT cached_response_pk PRIMARY KEY (key);
