#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE public.soundcloud_likes (
        id serial NOT NULL,
        artist varchar NULL,
        title varchar NULL,
        uploaded_at timestamp NULL,
        CONSTRAINT soundcloud_likes_pk PRIMARY KEY (id)
    );
    CREATE UNIQUE INDEX soundcloud_likes_id_idx ON public.soundcloud_likes (id);
EOSQL
