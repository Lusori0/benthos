package redis

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	tsFieldKey             = "key"
	tsFieldTimestamp       = "timestamp"
	tsFieldValue           = "value"
	tsFieldRetention       = "retention"
	tsFieldChunkSize       = "chunksize"
	tsFieldEncoding        = "encoding"
	tsFieldDuplicatePolicy = "duplicatepolicy"
	tsFieldLabels          = "labels"
)

func redisTimeseriesOutputConfig() *service.ConfigSpec {

	return service.NewConfigSpec().
		Stable().
		Summary(`Sets Redis timeseries data using the TS.ADD command.`).
		Description(output.Description(true, false, `
The field `+"`timestamp`"+` supports [interpolation functions](/docs/configuration/interpolation#bloblang-queries), allowing flexibility in setting the timestamp for the entry. If you want benthos to set the current time as a timestamp use `+"`${! timestamp_unix_milli() }`"+` as its value`)).
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
			service.NewDurationField(tsFieldRetention).
				Description("Specifies the duration for which Redis will keep the timeseries entries.").
				Examples("48h").
				Optional().
				Advanced(),
			service.NewIntField(tsFieldChunkSize).
				Description("Chunk size for new allocations. Must be between [48 .. 1048576] and in multiples of 8. For more information visit the [Redis docs](https://redis.io/commands/ts.create/).").
				Examples(16384).
				Optional().
				Advanced(),
			service.NewBoolField(tsFieldEncoding).
				Description("Specifies if Redis should compress the data.").
				Optional().
				Advanced(),
			service.NewStringEnumField(tsFieldDuplicatePolicy, "BLOCK", "FIRST", "LAST", "MIN", "MAX", "SUM").
				Description("Specifies how insertion of samples with identical timestamps should be handled. For more information visit the [Redis docs](https://redis.io/commands/ts.create/).").
				Optional().
				Advanced(),
			service.NewInterpolatedStringMapField(tsFieldLabels).
				Description("A map of key/value pairs to set as labels for timeseries keys.").
				Optional().
				Examples(map[string]string{"location": "${! this.location }", "provider": "aws"}).
				Advanced(),
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

	key             *service.InterpolatedString
	timestamp       *service.InterpolatedString
	value           *service.InterpolatedString
	retention       *time.Duration
	chunksize       *int
	encoding        *bool
	duplicatepolicy *string
	labels          *map[string]*service.InterpolatedString

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
	if conf.Contains(tsFieldRetention) {
		retention, err := conf.FieldDuration(tsFieldRetention)
		if err != nil {
			return nil, err
		}
		r.retention = &retention
	}
	if conf.Contains(tsFieldChunkSize) {
		chunksize, err := conf.FieldInt(tsFieldChunkSize)
		if err != nil {
			return nil, err
		}
		r.chunksize = &chunksize
	}
	if conf.Contains(tsFieldEncoding) {
		encoding, err := conf.FieldBool(tsFieldEncoding)
		if err != nil {
			return nil, err
		}
		r.encoding = &encoding
	}
	if conf.Contains(tsFieldDuplicatePolicy) {
		duplicatepolicy, err := conf.FieldString(tsFieldDuplicatePolicy)
		if err != nil {
			return nil, err
		}
		r.duplicatepolicy = &duplicatepolicy
	}
	if conf.Contains(tsFieldLabels) {
		labels, err := conf.FieldInterpolatedStringMap(tsFieldLabels)
		if err != nil {
			return nil, err
		}
		r.labels = &labels
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

	opt := &redis.TSOptions{}

	if r.retention != nil {
		opt.Retention = int(r.retention.Milliseconds())
	}

	if r.chunksize != nil {
		chunksize := *r.chunksize
		if chunksize%8 != 0 {
			return fmt.Errorf("chunksize must be multiple of 8")
		}
		if chunksize < 48 || chunksize > 1048576 {
			return fmt.Errorf("chunksize must be between [48 .. 1048576]")
		}
		opt.ChunkSize = chunksize
	}

	if r.encoding != nil {
		if *r.encoding {
			opt.Encoding = "COMPRESSED"
		} else {
			opt.Encoding = "UNCOMPRESSED"
		}
	}

	if r.duplicatepolicy != nil {
		opt.DuplicatePolicy = *r.duplicatepolicy
	}

	if r.labels != nil {
		labels := map[string]string{}
		for k, v := range *r.labels {
			labels[k], err = v.TryString(msg)
			if err != nil {
				return fmt.Errorf("label %v interpolation error: %w", k, err)
			}
		}
		opt.Labels = labels
	}

	if err := client.TSAddWithArgs(ctx, key, timestamp, value, opt).Err(); err != nil {
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
