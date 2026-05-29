
addCommandAlias(
  "a",
  "runMain com.resource.App\n" +
    "-DGRPC_PORT=8080\n" +
    "-Dakka.persistence.r2dbc.connection-factory.host=localhost\n" +
    "-Dakka.persistence.r2dbc.connection-factory.database=unique_resources\n" +
    "-Dakka.persistence.r2dbc.connection-factory.user=root\n" +
    "-Dakka.persistence.r2dbc.connection-factory.password=secret\n" +
    "-Dakka.remote.artery.canonical.port=2550\n" +
    "-Dakka.remote.artery.canonical.hostname=127.0.0.1\n" +
    "-DREQ_CONTACT_POINT_VAR=2\n"+
    "-Dakka.management.cluster.bootstrap.contact-point-discovery.discovery-method=config\n" +
    "-DCONTACT_POINTS=127.0.0.1,127.0.0.2"
)

//sudo ifconfig lo0 127.0.0.2 add
//sudo ifconfig lo0 alias 127.0.0.2 up
addCommandAlias(
  "b",
  "runMain com.resource.App\n" +
    "-DGRPC_PORT=8080\n" +
    "-Dakka.persistence.r2dbc.connection-factory.host=localhost\n" +
    "-Dakka.persistence.r2dbc.connection-factory.database=unique_resources\n" +
    "-Dakka.persistence.r2dbc.connection-factory.user=root\n" +
    "-Dakka.persistence.r2dbc.connection-factory.password=secret\n" +
    "-Dakka.remote.artery.canonical.port=2550\n" +
    "-Dakka.remote.artery.canonical.hostname=127.0.0.2\n" +
    "-DREQ_CONTACT_POINT_VAR=2\n"+
    "-Dakka.management.cluster.bootstrap.contact-point-discovery.discovery-method=config\n" +
    "-DCONTACT_POINTS=127.0.0.1,127.0.0.2"
)
