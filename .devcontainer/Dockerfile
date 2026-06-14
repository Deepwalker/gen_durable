FROM elixir:1.18-otp-27

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential \
      git \
      postgresql-client \
      inotify-tools \
 && rm -rf /var/lib/apt/lists/*

RUN mix local.hex --force && mix local.rebar --force

WORKDIR /workspace
