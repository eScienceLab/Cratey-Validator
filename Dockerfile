FROM python:3.11-slim

# git is needed by some dependencies; wget is only used when baking a profile.
RUN apt-get update && apt-get install -y git wget && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY cratey.py LICENSE /app/
COPY app /app/app

# Optionally bake an extra RO-Crate profile into the validator's bundled
# profiles directory. A plain build leaves PROFILES_ARCHIVE_URL empty and skips
# this entirely; the "with profiles" image build passes these as --build-arg.
# Baked profiles are then found automatically (no PROFILES_PATH needed).
ARG PROFILES_ARCHIVE_URL=""
ARG FIVE_SAFES_PROFILE_VERSION=""
ARG PY_VER=3.11
RUN if [ -n "$PROFILES_ARCHIVE_URL" ]; then \
        wget -O /tmp/profiles.tar.gz "$PROFILES_ARCHIVE_URL" && \
        tar -xzf /tmp/profiles.tar.gz \
            -C "/usr/local/lib/python${PY_VER}/site-packages/rocrate_validator/profiles/" \
            --strip-components=3 \
            "rocrate-validator-${FIVE_SAFES_PROFILE_VERSION}/rocrate_validator/profiles/five-safes-crate" && \
        rm /tmp/profiles.tar.gz ; \
    fi

RUN useradd -ms /bin/bash flaskuser
RUN chown -R flaskuser:flaskuser /app

USER flaskuser

EXPOSE 5000

CMD ["flask", "run", "--host=0.0.0.0"]

LABEL org.opencontainers.image.source="https://github.com/eScienceLab/Cratey-Validator"
LABEL org.cratey.five-safes-profile-version="${FIVE_SAFES_PROFILE_VERSION}"
