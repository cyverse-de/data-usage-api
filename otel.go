package main

import (
	"context"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
)

// Create a Resource for this service
func otelResource(ctx context.Context, serviceName string) (*resource.Resource, error) {
	var res *resource.Resource
	attrs := []attribute.KeyValue{
		semconv.ServiceNameKey.String(serviceName),
		semconv.ServiceNamespaceKey.String("org.cyverse"),
	}
	instanceID, err := os.Hostname()
	if err != nil {
		newUUID, err := uuid.NewRandom()
		if err == nil {
			instanceID = newUUID.String()
		}
	}
	if instanceID != "" {
		attrs = append(attrs, semconv.ServiceInstanceIDKey.String(instanceID))
	}

	res, err = resource.New(ctx,
		resource.WithSchemaURL(semconv.SchemaURL),
		resource.WithHost(),
		resource.WithProcess(),
		resource.WithContainer(),
		resource.WithTelemetrySDK(),
		resource.WithAttributes(attrs...))
	if err != nil {
		return nil, err
	}

	return res, nil
}

// Get a TracerProvider using Jaeger as the exporter
func jaegerTracerProvider(ctx context.Context, serviceName, url string) (*tracesdk.TracerProvider, error) {
	// Create the Jaeger exporter
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(url)))
	if err != nil {
		return nil, err
	}

	res, err := otelResource(ctx, serviceName)
	if err != nil {
		return nil, err
	}

	tp := tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(exp),
		tracesdk.WithResource(res),
	)

	return tp, nil
}

// Get a TracerProvider using the OTEL_* environment variables to determine
// configuration. Currently, only supports a Jaeger exporter.
func TracerProviderFromEnv(ctx context.Context, serviceName string, onErr func(error)) func() {
	var (
		tracerProvider *tracesdk.TracerProvider
		err            error
	)

	otelTracesExporter := os.Getenv("OTEL_TRACES_EXPORTER")
	if otelTracesExporter == "jaeger" {
		jaegerEndpoint := os.Getenv("OTEL_EXPORTER_JAEGER_ENDPOINT")
		if jaegerEndpoint == "" {
			onErr(errors.New("Jaeger set as OpenTelemetry trace exporter, but no Jaeger endpoint configured."))
			return func() {}
		} else {
			tracerProvider, err = jaegerTracerProvider(ctx, serviceName, jaegerEndpoint)
			if err != nil {
				onErr(err)
				return func() {}
			}
		}
	}

	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return shutdownTracerProviderFn(tracerProvider, ctx, onErr)
}

func shutdownTracerProviderFn(tracerProvider *tracesdk.TracerProvider, tracerContext context.Context, onErr func(error)) func() {
	return func() {
		ctx, cancel := context.WithTimeout(tracerContext, time.Second*5)
		defer cancel()
		if err := tracerProvider.Shutdown(ctx); err != nil {
			onErr(err)
		}
	}
}
