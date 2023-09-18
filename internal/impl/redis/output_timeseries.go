package redis

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	tsFieldKey       = "key"
	tsFieldTimestamp = "timestamp"
	tsFieldValue     = "value"
)

func redisTimeseriesOutputConfig() *service.ConfigSpec {

	return service.NewConfigSpec().
		Stable().
		Summary(`Sets Redis timeseries data using the TS.ADD command.`).
		Description(output.Description(true, false, `
The field `+"`key`"+` supports [interpolation functions](/docs/configuration/interpolation#bloblang-queries), allowing you to create a unique key for each message.

The field `+"`timestamp`"+` supports [interpolation functions](/docs/configuration/interpolation#bloblang-queries), allowing flexibility in setting the timestamp for the entry. If you want benthos to set the current time as a timestamp use `+"`${! timestamp_unix_milli() }`"+` as the value

The field `+"`value`"+` supports [interpolation functions](/docs/configuration/interpolation#bloblang-queries).`)).
		Categories("Services").
		Fields(clientFields()...).
		Fields(
			service.NewInterpolatedStringField(tsFieldKey).
				Description("Key of the timeseries entry, interpolations can be used.").
				Examples("cpu_usage:${! this.id }", "my_key"),
			service.NewInterpolatedStringField(tsFieldTimestamp).
				Description("Timestamp of the entry. You can tell Benthos to set the current timestamp with a bloblang expression. Must be an integer.").
				Examples("${! timestamp_unix_milli() }", "${! this.timestamp }", "1695047626631"),
			service.NewInterpolatedStringField(tsFieldValue).
				Description("Value of the timeseries entry. Must be a number.").
				Examples("${! this.cpu_usage }", "3.1415"),
			service.NewOutputMaxInFlightField(),
		)

}

func init() {
	err := service.RegisterOutput(
		"redis_timeseries", redisTimeseriesOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			out, err = newRedisTimeseriesWriter(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

type redisTimeseriesWriter struct {
	log *service.Logger

	key       *service.InterpolatedString
	timestamp *service.InterpolatedString
	value     *service.InterpolatedString

	clientCtor func() (redis.UniversalClient, error)
	client     redis.UniversalClient
	connMut    sync.RWMutex
}

func newRedisTimeseriesWriter(conf *service.ParsedConfig, mgr *service.Resources) (r *redisTimeseriesWriter, err error) {
	r = &redisTimeseriesWriter{
		clientCtor: func() (redis.UniversalClient, error) {
			return getClient(conf)
		},
		log: mgr.Logger(),
	}
	if _, err = getClient(conf); err != nil {
		return
	}
	if r.key, err = conf.FieldInterpolatedString(tsFieldKey); err != nil {
		return
	}
	if r.timestamp, err = conf.FieldInterpolatedString(tsFieldTimestamp); err != nil {
		return
	}
	if r.value, err = conf.FieldInterpolatedString(tsFieldValue); err != nil {
		return
	}
	return
}

func (r *redisTimeseriesWriter) Connect(ctx context.Context) error {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	client, err := r.clientCtor()
	if err != nil {
		return err
	}
	if _, err = client.Ping(ctx).Result(); err != nil {
		return err
	}
	r.log.Info("Setting messages as timeseries entries to Redis")
	r.client = client
	return nil
}

func (r *redisTimeseriesWriter) Write(ctx context.Context, msg *service.Message) error {
	r.connMut.RLock()
	client := r.client
	r.connMut.RUnlock()

	if client == nil {
		return service.ErrNotConnected
	}

	key, err := r.key.TryString(msg)
	if err != nil {
		return fmt.Errorf("key interpolation error: %w", err)
	}

	timestampStr, err := r.timestamp.TryString(msg)
	if err != nil {
		return fmt.Errorf("timestamp interpolation error: %w", err)
	}
	timestamp, err := strconv.Atoi(timestampStr)
	if err != nil {
		return fmt.Errorf("timestamp integer conversion error: %w", err)
	}

	valueStr, err := r.value.TryString(msg)
	if err != nil {
		return fmt.Errorf("value interpolation error: %w", err)
	}
	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		return fmt.Errorf("value float conversion error: %w", err)
	}

	if err := client.TSAdd(ctx, key, timestamp, value).Err(); err != nil {
		_ = r.disconnect()
		r.log.Errorf("Error from redis: %v\n", err)
		return service.ErrNotConnected
	}
	return nil
}

func (r *redisTimeseriesWriter) disconnect() error {
	r.connMut.Lock()
	defer r.connMut.Unlock()
	if r.client != nil {
		err := r.client.Close()
		r.client = nil
		return err
	}
	return nil
}

func (r *redisTimeseriesWriter) Close(context.Context) error {
	return r.disconnect()
}
