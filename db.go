package burrowdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
)

var (
	ErrInvalidValueType = errors.New("invalid value type")
	ErrNoIDField        = errors.New("value has no ID field")
	ErrMultipleIDFields = errors.New("value has multiple ID fields")
	ErrNoSuchEntity     = errors.New("no such entity exists")
	ErrNonPointerDst    = errors.New("dst is not a pointer")
)

const (
	structTagName = "burrowdb" // Struct tag key.
	idFieldName   = "ID"       // Name required for a field or struct tag to specify ID field.
)

// BurrowDB is a database built for golang in golang.
type BurrowDB struct {
	dir string // directory where files will be stored.
}

// newDBOption is an option which can be passed to NewDB to change the behaviour
// of the initialisation.
type newDBOption func(*BurrowDB) error

// WithDir specifies the directory where entries will be stored.
func WithDir(dir string) newDBOption {
	return func(db *BurrowDB) error {
		db.dir = dir
		return nil
	}
}

// NewDB returns a new BurrowDB instance with the passed options.
//
// If no directory or target is passed, the db will default to using
// a localstore at ./burrow.
func NewDB(opts ...newDBOption) (*BurrowDB, error) {
	db := &BurrowDB{}
	for _, opt := range opts {
		err := opt(db)
		if err != nil {
			return nil, fmt.Errorf("error applying option: %v", err)
		}
	}

	if db.dir == "" {
		db.dir = "burrow"
	}

	err := os.MkdirAll(db.dir, 0777)
	if err != nil {
		return nil, fmt.Errorf("unable to create directory (%q): %v", db.dir, err)
	}

	return db, nil
}

// Put takes a value and puts it into the db. This will overwrite any existing
// object with the same ID.
//
// The value must be a struct type. To specify the ID field for the object, the
// field should either be called ID or the struct tag should be `burrowdb: "ID"`
func (db *BurrowDB) Put(v any) error {
	_type := reflect.TypeOf(v)
	if _type.Kind() != reflect.Struct {
		return ErrInvalidValueType
	}

	fields := reflect.VisibleFields(_type)
	var idField *reflect.StructField
	for _, field := range fields {
		if field.Name == idFieldName {
			if idField != nil {
				return ErrMultipleIDFields
			}
			idField = &field
			continue
		}

		if field.Tag.Get(structTagName) == idFieldName {
			if idField != nil {
				return ErrMultipleIDFields
			}
			idField = &field
		}
	}

	if idField == nil {
		return ErrNoIDField
	}

	// Marshal into JSON.
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("unable to marshal value: %v", err)
	}

	_v := reflect.ValueOf(v)

	// Write File.
	typeDir := fmt.Sprintf("%s/%s", db.dir, _type.Name())
	err = os.MkdirAll(typeDir, 0777)
	if err != nil {
		return fmt.Errorf("unable to create type dir: %w", err)
	}

	filename := fmt.Sprintf("%s/%v", typeDir, _v.FieldByName(idField.Name))
	err = os.WriteFile(filename, data, 0666)
	if err != nil {
		return fmt.Errorf("unable to write file: %w", err)
	}

	return nil
}

// GetByID gets the entity with the type of the passed destination with the
// passed ID.
func (db *BurrowDB) GetByID(dst any, id any) error {
	// Make sure that the dst is a pointer type.
	_type := reflect.TypeOf(dst)
	if _type.Kind() != reflect.Pointer {
		return ErrNonPointerDst
	}

	filename := fmt.Sprintf("%s/%s/%v", db.dir, _type.Elem().Name(), id)

	data, err := os.ReadFile(filename)
	if errors.Is(err, os.ErrNotExist) {
		return ErrNoSuchEntity
	} else if err != nil {
		return fmt.Errorf("unable to get entity: %w", err)
	}

	err = json.Unmarshal(data, dst)
	if err != nil {
		return fmt.Errorf("unable to unmarshal data: %w", err)
	}

	return nil

}
