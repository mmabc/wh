CREATE TABLE schema1.log
(
  id bigint NOT NULL AUTO_INCREMENT,
  request_time timestamp,
  ip text,
  request_type text,
  status int,
  browser text,
  CONSTRAINT description_pkey PRIMARY KEY (id)
);

