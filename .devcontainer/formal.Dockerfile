# Formal-methods toolchain for the protocol models in ./formal:
#   Quint (spec language + simulator), Apalache (symbolic model checker: `quint verify`),
#   TLC (explicit-state model checker: `quint verify --backend=tlc`).
#
# Quint pins the Apalache/TLC versions it drives and fetches them on first use; the warm-up
# step below does that at build time so the container works offline afterwards.
FROM node:22-bookworm

# Apalache >= 0.62 is built for Java 21 (class file 65); Debian bookworm ships 17.
COPY --from=eclipse-temurin:21-jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="/opt/java/openjdk/bin:${PATH}"
# login shells (bash -l) reset PATH from /etc/profile — make java resolvable regardless
RUN ln -s /opt/java/openjdk/bin/java /usr/local/bin/java

ARG QUINT_VERSION=0.34.0
RUN npm install -g @informalsystems/quint@${QUINT_VERSION}

# Warm Quint's tool cache (Apalache distribution + TLC) with a trivial spec.
RUN mkdir -p /tmp/warm && cd /tmp/warm \
 && printf 'module w {\n  var x: int\n  action init = x'"'"' = 0\n  action step = x'"'"' = (x + 1) %% 2\n  val ok = x < 2\n}\n' > w.qnt \
 && quint verify w.qnt --invariant=ok --max-steps=2 \
 && quint verify w.qnt --invariant=ok --backend=tlc \
 && rm -rf /tmp/warm

WORKDIR /workspace/formal
