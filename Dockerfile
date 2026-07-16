# syntax=docker/dockerfile:1
FROM python:3.11-slim

# git is needed by some dependencies; wget is only used when baking a profile.
RUN apt-get update && apt-get install -y git wget && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY wsgi.py LICENSE /app/
COPY app /app/app

# Optionally fetch an extra RO-Crate profile into a normal directory. It is
# *added* to the bundled profiles at runtime via EXTRA_PROFILES_PATH.
# A plain build leaves PROFILES_ARCHIVE_URL empty
# and skips this; the "with profiles" image build passes it as --build-arg.
ARG PROFILES_ARCHIVE_URL=""
ARG FIVE_SAFES_PROFILE_VERSION=""
# Set EXTRA_PROFILES_PATH only for the profiles build (passed as a build arg).
ARG EXTRA_PROFILES_PATH=""
ENV EXTRA_PROFILES_PATH=${EXTRA_PROFILES_PATH}
ENV CACHE_PATH=/app/.rocrate-cache
RUN <<EOF_PROFILES
set -e
if [ -n "$PROFILES_ARCHIVE_URL" ]; then
    mkdir -p /app/extra-profiles
    wget -O /tmp/profiles.tar.gz "$PROFILES_ARCHIVE_URL"
    tar -xzf /tmp/profiles.tar.gz \
        -C /app/extra-profiles \
        --strip-components=3 \
        "rocrate-validator-${FIVE_SAFES_PROFILE_VERSION}/rocrate_validator/profiles/five-safes-crate"
    rm /tmp/profiles.tar.gz
fi
EOF_PROFILES

# Pre-populate the HTTP cache so opt-in offline validation
# (VALIDATION_OFFLINE=true) works without network at runtime.
RUN <<EOF_CACHE_WARM
set -e
if [ -n "$EXTRA_PROFILES_PATH" ]; then
    rocrate-validator cache warm --all-profiles \
        --extra-profiles-path "$EXTRA_PROFILES_PATH" --cache-path "$CACHE_PATH"
else
    rocrate-validator cache warm --all-profiles --cache-path "$CACHE_PATH"
fi
EOF_CACHE_WARM

RUN useradd -ms /bin/bash flaskuser
RUN chown -R flaskuser:flaskuser /app

USER flaskuser

EXPOSE 5000

CMD ["flask", "run", "--host=0.0.0.0"]

LABEL org.opencontainers.image.source="https://github.com/eScienceLab/RO-Crate-Validation-Service"
LABEL org.ro-crate-validation-service.five-safes-profile-version="${FIVE_SAFES_PROFILE_VERSION}"
