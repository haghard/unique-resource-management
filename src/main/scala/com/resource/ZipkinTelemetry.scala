package com.resource

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.{AttributeKey, Attributes}
import io.opentelemetry.exporter.zipkin.ZipkinSpanExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.`export`.SimpleSpanProcessor

object ZipkinTelemetry {

  def create(ip: String, port: Int): (OpenTelemetry, SdkTracerProvider) = {

    val endpoint            = s"http://$ip:$port/api/v2/spans"
    val zipkinExporter      = ZipkinSpanExporter.builder().setEndpoint(endpoint).build()
    val serviceNameResource =
      Resource.create(Attributes.of(AttributeKey.stringKey("service.name"), App.AkkaSystemName))

    // Set to process the spans by the Zipkin Exporter
    val tracerProvider = SdkTracerProvider
      .builder()
      .addSpanProcessor(SimpleSpanProcessor.create(zipkinExporter))
      // .addSpanProcessor(SimpleSpanProcessor.create(io.opentelemetry.exporter.logging.LoggingSpanExporter.create()))
      .setResource(serviceNameResource)
      .build()

    val telemetryDsk = OpenTelemetrySdk
      .builder()
      .setTracerProvider(tracerProvider)
      .buildAndRegisterGlobal()

    // add a shutdown hook to shut down the SDK
    //Runtime.getRuntime().addShutdownHook(new Thread(() => tracerProvider.close()))

    (telemetryDsk, tracerProvider)
  }
}
