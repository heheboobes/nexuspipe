package logger

import (
	"os"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	instance *zap.Logger
	sugar    *zap.SugaredLogger
	once     sync.Once
	level    zap.AtomicLevel
)

type Config struct {
	Level      string
	Format     string
	OutputPath string
	ErrorPath  string
	AddSource  bool
	Sampling   bool
	Encoding   string
}

func NewLogger(cfg Config) (*zap.Logger, error) {
	var err error

	once.Do(func() {
		level = zap.NewAtomicLevel()
		if err = level.UnmarshalText([]byte(cfg.Level)); err != nil {
			level.SetLevel(zapcore.InfoLevel)
		}

		encoderConfig := zapcore.EncoderConfig{
			TimeKey:        "timestamp",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			FunctionKey:    zapcore.OmitKey,
			MessageKey:     "message",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     timeEncoder,
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		}

		var encoder zapcore.Encoder
		if cfg.Format == "json" || cfg.Encoding == "json" {
			encoder = zapcore.NewJSONEncoder(encoderConfig)
		} else {
			encoder = zapcore.NewConsoleEncoder(encoderConfig)
		}

		outputPaths := []string{"stdout"}
		errorPaths := []string{"stderr"}

		if cfg.OutputPath != "" && cfg.OutputPath != "stdout" {
			outputPaths = append(outputPaths, cfg.OutputPath)
		}
		if cfg.ErrorPath != "" && cfg.ErrorPath != "stderr" {
			errorPaths = append(errorPaths, cfg.ErrorPath)
		}

		cores := make([]zapcore.Core, 0)

		stdoutWriteSyncer := zapcore.Lock(os.Stdout)
		stderrWriteSyncer := zapcore.Lock(os.Stderr)

		cores = append(cores, zapcore.NewCore(encoder, stdoutWriteSyncer, level))

		if cfg.ErrorPath != "stderr" {
			fileWriter, openErr := os.OpenFile(cfg.ErrorPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if openErr == nil {
				cores = append(cores, zapcore.NewCore(encoder, zapcore.AddSync(fileWriter), zapcore.ErrorLevel))
			}
		}

		if len(errorPaths) > 0 {
			cores = append(cores, zapcore.NewCore(encoder, stderrWriteSyncer, zapcore.ErrorLevel))
		}

		tee := zapcore.NewTee(cores...)

		opts := []zap.Option{
			zap.AddCaller(),
			zap.AddStacktrace(zapcore.ErrorLevel),
		}

		if cfg.AddSource {
			opts = append(opts, zap.AddCallerSkip(1))
		}

		instance = zap.New(tee, opts...)

		if cfg.Sampling {
			instance = instance.WithOptions(zap.WrapCore(func(c zapcore.Core) zapcore.Core {
				return zapcore.NewSamplerWithOptions(c, time.Second, 100, 100)
			}))
		}

		sugar = instance.Sugar()
	})

	if err != nil {
		return nil, err
	}

	return instance, nil
}

func timeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format("2006-01-02T15:04:05.000Z0700"))
}

func GetLogger() *zap.Logger {
	if instance == nil {
		logger, _ := NewLogger(Config{
			Level:     "info",
			Format:    "console",
			AddSource: true,
		})
		return logger
	}
	return instance
}

func GetSugaredLogger() *zap.SugaredLogger {
	if sugar == nil {
		GetLogger()
	}
	return sugar
}

func SetLogLevel(lvl string) error {
	var l zapcore.Level
	if err := l.UnmarshalText([]byte(lvl)); err != nil {
		return err
	}
	if level != nil {
		level.SetLevel(l)
	}
	return nil
}

func WithContext(fields ...zap.Field) *zap.Logger {
	return GetLogger().With(fields...)
}

func WithTags(tags map[string]string) *zap.Logger {
	fields := make([]zap.Field, 0, len(tags))
	for k, v := range tags {
		fields = append(fields, zap.String(k, v))
	}
	return GetLogger().With(fields...)
}

func Sync() error {
	if instance != nil {
		return instance.Sync()
	}
	return nil
}

func Debug(msg string, fields ...zap.Field) {
	GetLogger().Debug(msg, fields...)
}

func Info(msg string, fields ...zap.Field) {
	GetLogger().Info(msg, fields...)
}

func Warn(msg string, fields ...zap.Field) {
	GetLogger().Warn(msg, fields...)
}

func Error(msg string, fields ...zap.Field) {
	GetLogger().Error(msg, fields...)
}

func Fatal(msg string, fields ...zap.Field) {
	GetLogger().Fatal(msg, fields...)
}
