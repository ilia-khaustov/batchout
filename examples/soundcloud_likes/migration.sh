#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE public.soundcloud_likes (
        id serial NOT NULL,
        artist text NULL,
        title text NULL,
        uploaded_at timestamp NULL,
        liked_by text NULL,
        batched_at timestamp default now()
        CONSTRAINT soundcloud_likes_pk PRIMARY KEY (id)
    );
    CREATE UNIQUE INDEX soundcloud_likes_id_idx ON public.soundcloud_likes (id);
EOSQL
