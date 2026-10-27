# Fix: OCI uploads fail inside Kafka Connect but succeed standalone

This change fixes a Jersey service-provider classloader conflict that stopped the
OCI sink from uploading records inside Kafka Connect. The failure happened during
HTTP client initialization, before the request reached OCI. A signed `curl` call
and a standalone Java SDK call to the same destination succeeded.

## Observed failure

Kafka received the events, and the connector and its task reported `RUNNING`, but
uploads repeatedly failed with:

```text
RetriableException: OCI Log Events request failed: status=-1, opcRequestId=null
```

The OCI consumer offset did not advance. `-1` was a client-side SDK failure, not
an HTTP status returned by OCI. The uploader converted the SDK exception into a
Connect exception containing only the status and request ID, hiding its cause.

Reproducing an upload with Kafka's actual `PluginClassLoader` exposed:

```text
java.lang.ClassCastException:
  org.glassfish.jersey.server.wadl.internal.WadlAutoDiscoverable
  cannot be cast to org.glassfish.jersey.internal.spi.AutoDiscoverable
```

The server provider came from the worker's application classloader. The interface
it was being cast to came from the plugin classloader.

## Why standalone calls worked

The reproduced worker used Jersey 3.1.10 for Kafka Connect's REST server. The OCI
plugin packaged its own Jersey 3.0.8 client dependencies. The important conflict
was between classes loaded by different classloaders: Java treats an interface
loaded by one classloader as a different type from the same interface name loaded
by another.

Kafka's plugin classloader also exposes parent `META-INF/services/` descriptors.
While initializing the HTTP client during
`client.uploadLogEventsFile(request)`, Jersey discovered the worker's server-side
providers through those descriptors. It then tried to use a worker provider with
the plugin's copy of Jersey's provider interface and threw `ClassCastException`.

The standalone process did not contain Kafka Connect's server providers, so this
cross-classloader discovery did not occur. OCI wrapped the worker's local failure
as a `BmcException` with status `-1`, which the connector retried.

## Implementation

The fix scopes SDK service discovery to the plugin's own distribution:

1. [SdkClassLoaderScope.java](src/main/java/com/oracle/observability/kafka/connect/SdkClassLoaderScope.java)
   temporarily sets the thread context classloader for SDK operations. With a
   `URLClassLoader`, service-resource lookup uses the plugin's `findResource()` and
   `findResources()` methods, which search its own URLs without including parent
   service descriptors. Class loading and other resource lookup continue to
   delegate to the plugin loader, retaining the worker's shared Kafka and SLF4J APIs.
2. If the plugin has no descriptor for a service, lookup returns the packaged
   [empty-service-provider](src/main/resources/com/oracle/observability/kafka/connect/empty-service-provider)
   resource. Jersey otherwise falls back to its defining classloader when the
   context loader returns no resources, which would expose worker providers again.
   The empty descriptor prevents that fallback while declaring no providers.
3. [OciSdkLogEventsUploader.java](src/main/java/com/oracle/observability/kafka/connect/OciSdkLogEventsUploader.java)
   opens the scope around both client construction and the upload call. Jersey
   initializes parts of its runtime lazily, so scoping construction alone is
   insufficient. Try-with-resources restores the previous thread context
   classloader on success or failure.

The scope falls back to the plugin's defining classloader when it is not a
`URLClassLoader`. No global Jersey settings or worker dependencies are changed.
The existing proxy configuration, authentication, batching, retry behavior, and
offset handling remain in place.

## Regression coverage and verification

[PackagedPluginIT.java](src/test/java/com/oracle/observability/kafka/connect/PackagedPluginIT.java)
now performs an actual signed upload from the assembled plugin ZIP to a local
HTTP server. Its simulated worker exposes an incompatible Jersey
`ForcedAutoDiscoverable` provider through a parent service descriptor. This
reproduces the provider leakage that an initialization-only test missed.

Before the fix, the request failed with `status=-1`. With the fix, the local
server receives the upload and the test also checks that the worker's context
classloader is restored afterward. The test uses generated credentials and a
loopback endpoint; it requires no real OCI credentials.

Validation completed:

- The port onto the proxy-support revision passed **57 checks**: 56 unit/HTTP
  checks and one packaged-plugin integration check, with no failures, errors, or
  skipped tests.
- A separate reproduction using Kafka's actual `PluginClassLoader` successfully
  uploaded to OCI after applying the same fix.
- In the live lab, the corrected worker uploaded 50 queued records and 10 fresh
  events. The OCI committed offset reached the topic end at `1548`, giving lag 0.
  The local file sink received all 10 fresh events unchanged.
- The existing `oci-sink.local.json` was preserved byte-for-byte.

These live results verify OCI API acceptance. Log Analytics indexing, parsing,
and search visibility require a separate check in Log Explorer.

## Build and deployment

With Java 17 or later, run from the repository root:

```bash
mvn clean verify
```

The build produces:

```text
target/kafka-connect-oracleobservability-0.1.0-SNAPSHOT-plugin.zip
```

Replace the installed plugin distribution with the rebuilt ZIP's contents,
preserving its `lib/` directory and all runtime JARs, and restart the Connect
workers so they load the new classes. Preserve the existing
`oci-sink.local.json`, connector names, Kafka data, and Connect internal topics.
Existing committed offsets can then be retained while queued records are retried.

Confirm both `Log Events accepted` messages and advancing committed offsets;
`RUNNING` status alone does not establish successful delivery.
