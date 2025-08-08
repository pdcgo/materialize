package stat_replica

import (
	"context"
	"errors"
	"fmt"
	"reflect"
)

type coderMapper struct {
	data map[string]interface{}
}

var CODER_KEY = "coder_mapper"
var ErrCoderMapperNotFound = errors.New("coder mapper not found in context when registering code")
var ErrCoderNotFound = errors.New("coder not found")

func ContextWithCoder(ctx context.Context) context.Context {
	mapper := coderMapper{
		data: map[string]interface{}{},
	}

	return context.WithValue(ctx, CODER_KEY, &mapper)
}

type CoderReg struct {
	Meta  *SourceMetadata
	Coder interface{}
}

func RegisterCoderSources(ctx context.Context, codregs ...*CoderReg) error {
	var err error

	for _, reg := range codregs {
		err = RegisterCoderSource(ctx, reg.Meta, reg.Coder)
		if err != nil {
			return err
		}
	}

	return err
}

func RegisterCoderSource(ctx context.Context, meta *SourceMetadata, coder interface{}) error {
	var err error
	key := meta.PrefixKey()

	mapper, ok := ctx.Value(CODER_KEY).(*coderMapper)
	if !ok {
		return ErrCoderMapperNotFound
	}

	_, ok = mapper.data[key]
	if ok {
		return fmt.Errorf("coder %s already registered", key)
	}

	mapper.data[key] = coder
	return err
}

func RegisterCoder(ctx context.Context, key string, coder interface{}) error {
	var err error

	mapper, ok := ctx.Value(CODER_KEY).(*coderMapper)
	if !ok {
		return ErrCoderMapperNotFound
	}

	_, ok = mapper.data[key]
	if ok {
		return fmt.Errorf("coder %s already registered", key)
	}

	mapper.data[key] = coder
	return err
}

func GetCoder(ctx context.Context, key string) (interface{}, error) {
	mapper, ok := ctx.Value(CODER_KEY).(*coderMapper)
	if !ok {
		return nil, ErrCoderMapperNotFound
	}

	var coder interface{}
	coder, ok = mapper.data[key]
	if !ok {
		return coder, ErrCoderNotFound
	}

	newcoder := NewEmptyFromStruct(coder)
	return newcoder, nil
}

func NewEmptyFromStruct(input interface{}) interface{} {
	v := reflect.ValueOf(input)

	// If input is a pointer, get its element
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	// Create a new zero value of the same type
	newVal := reflect.New(v.Type()).Interface()

	return newVal
}
