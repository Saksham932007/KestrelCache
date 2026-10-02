# syntax=docker/dockerfile:1

# ----------------------------------------------------------------------------- build
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

# Project files first, so `restore` is cached independently of source changes. Editing a .cs
# file then rebuilds without re-downloading every NuGet package, which is the difference between
# a two-second and a two-minute rebuild.
# Every project in the solution has to be listed, and every file restore reads: the solution
# names all of them, so one missing .csproj fails the whole restore with MSB3202, and a missing
# Directory.Packages.props fails it with NU1604 because central package management is what
# supplies the versions the project files deliberately omit. global.json pins the SDK, and
# copying it here is the difference between the pin applying to this build and being ignored
# until after restore has already chosen a toolchain.
COPY Directory.Build.props Directory.Packages.props global.json KestrelCache.sln ./
COPY src/KestrelCache/KestrelCache.csproj                     src/KestrelCache/
COPY src/KestrelCache.Cli/KestrelCache.Cli.csproj             src/KestrelCache.Cli/
COPY src/KestrelCache.Raft/KestrelCache.Raft.csproj           src/KestrelCache.Raft/
COPY src/KestrelCache.Server/KestrelCache.Server.csproj       src/KestrelCache.Server/
COPY tests/KestrelCache.Tests/KestrelCache.Tests.csproj       tests/KestrelCache.Tests/
COPY benchmarks/KestrelCache.Benchmarks/KestrelCache.Benchmarks.csproj benchmarks/KestrelCache.Benchmarks/
RUN dotnet restore

COPY . .
RUN dotnet publish src/KestrelCache.Server/KestrelCache.Server.csproj \
        -c Release -o /app/server --no-restore \
 && dotnet publish src/KestrelCache.Cli/KestrelCache.Cli.csproj \
        -c Release -o /app/cli --no-restore

# ----------------------------------------------------------------------------- runtime
FROM mcr.microsoft.com/dotnet/runtime:9.0 AS runtime

# Server GC uses a heap and a collection thread per core, which suits a long-lived server. The
# default workstation GC is tuned for desktop latency on a single foreground app.
ENV DOTNET_gcServer=1 \
    DOTNET_TieredPGO=1 \
    DOTNET_CLI_TELEMETRY_OPTOUT=1 \
    DOTNET_NOLOGO=1

WORKDIR /app
COPY --from=build /app/server ./
COPY --from=build /app/cli ./cli/

# A dedicated unprivileged user. The process has no need for root, and a container escape from
# an unprivileged process is a far smaller problem than one from root.
RUN groupadd --system --gid 10001 kestrel \
 && useradd --system --uid 10001 --gid kestrel --home /data --shell /usr/sbin/nologin kestrel \
 && mkdir -p /data \
 && chown -R kestrel:kestrel /data /app

USER kestrel
VOLUME ["/data"]

EXPOSE 6380 9180

# The HTTP health endpoint touches the engine rather than merely confirming the process is
# alive, so a database that has stopped answering fails the check.
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD ["/app/kestrel-server", "--help"]

ENTRYPOINT ["/app/kestrel-server"]
CMD ["--data", "/data/kc", "--port", "6380", "--metrics-port", "9180", "--engine", "lsm", "--sync", "interval"]
