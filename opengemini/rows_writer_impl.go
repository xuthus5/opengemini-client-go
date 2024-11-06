package opengemini

import (
	"context"
	"errors"
	"fmt"
	"github.com/libgox/addr"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/opengemini-client-go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sort"
	"sync"
	"time"
)

type RowsWriterConfig struct {
	// Addresses Configure the gRPC endpoints for the openGemini service.
	// This parameter is required.
	Addresses []addr.Address
	// BatchConfig Controls whether automatic batching of messages is enabled for the RowsWriter, if this
	// option is not set, the user needs to manually execute the Flush(), otherwise the program will OutOfMemory
	BatchConfig *BatchConfig
}

type RowsWriterClient struct {
	Addresses   []addr.Address
	BatchConfig *BatchConfig
}

func NewRowsWriterClient(cfg *RowsWriterConfig) *RowsWriterClient {
	return &RowsWriterClient{
		Addresses:   cfg.Addresses,
		BatchConfig: cfg.BatchConfig,
	}
}

func (rwc RowsWriterClient) CreateRowsWriter(database, retentionPolicy, measurement string) RowsWriter {
	return NewTransform(database, retentionPolicy, measurement)
}

type RowsWriter interface {
	AppendPoint(point *Point) error
	AppendPoints(points []*Point) (failedParts []*Point, err error)
	Flush(ctx context.Context) error
}

const (
	TimeColumnName = "time"
)

var (
	ErrInvalidTimeColumn = errors.New("key can't be time")
	ErrEmptyName         = errors.New("empty name not allowed")
	ErrInvalidFieldType  = errors.New("invalid field type")
	ErrUnknownFieldType  = errors.New("unknown field type")
)

type Column struct {
	schema record.Field
	cv     record.ColVal
}

type Transforms struct {
	mux  sync.RWMutex
	data map[string]map[string]map[string]*Transform
}

func (t *Transforms) get(database, rp, mst string) *Transform {
	t.mux.Lock()
	defer t.mux.Unlock()
	if len(t.data) == 0 {
		t.data = make(map[string]map[string]map[string]*Transform)
	}
	dbNode, ok := t.data[database]
	if !ok {
		dbNode = make(map[string]map[string]*Transform)
		t.data[database] = dbNode
	}
	rpNode, ok := dbNode[rp]
	if !ok {
		rpNode = make(map[string]*Transform)
		dbNode[rp] = rpNode
	}
	mstNode, ok := rpNode[mst]
	if !ok {
		mstNode = NewTransform(database, rp, mst)
		rpNode[mst] = mstNode
	}
	return mstNode
}

type Transform struct {
	Database        string
	RetentionPolicy string
	Measurement     string
	RowCount        int
	MinTime         int64
	MaxTime         int64
	mux             sync.RWMutex
	Columns         map[string]*Column
	fillChecker     map[string]bool
}

// NewTransform creates a new Transform instance with configuration
func NewTransform(database, rp, mst string) *Transform {
	return &Transform{
		Database:        database,
		RetentionPolicy: rp,
		Measurement:     mst,
		Columns:         make(map[string]*Column),
		fillChecker:     make(map[string]bool),
	}
}

// AppendPoint writes data by row with improved error handling
func (t *Transform) AppendPoint(point *Point) error {
	t.mux.Lock()
	defer t.mux.Unlock()

	// process tags
	if err := t.processTagColumns(point.Tags); err != nil {
		return err
	}

	// process fields
	if err := t.processFieldColumns(point.Fields); err != nil {
		return err
	}

	// process timestamp
	if err := t.processTimestamp(point.Time); err != nil {
		return err
	}

	t.RowCount++

	// fill another field or tag
	if err := t.processMissValueColumns(); err != nil {
		return err
	}

	return nil
}

func (t *Transform) AppendPoints(points []*Point) (failedParts []*Point, err error) {
	for _, point := range points {
		if failedErr := t.AppendPoint(point); failedErr != nil {
			failedParts = append(failedParts, point)
			err = errors.Join(err, failedErr)
		}
	}
	return failedParts, err
}

func (t *Transform) Flush(ctx context.Context) error {
	conn, err := grpc.NewClient("127.0.0.1:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	rawRecord, err := t.ToSrvRecords()
	if err != nil {
		panic(err)
	}

	var buff []byte
	buff, err = rawRecord.Marshal(buff)
	if err != nil {
		panic(err)
	}

	client := proto.NewWriteRowsServiceClient(conn)
	response, err := client.WriteRows(ctx, &proto.WriteRowsRequest{
		Version:  0,
		Database: t.Database,
		Rp:       t.RetentionPolicy,
		Username: "admin",
		Password: "Admin@123",
		Rows: &proto.Rows{
			Measurement:  t.Measurement,
			MinTime:      t.MinTime,
			MaxTime:      t.MaxTime,
			CompressAlgo: 0,
			Block:        buff,
		},
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("response: %+v\n", response)
	return nil
}

func (t *Transform) createColumn(name string, fieldType int) (*Column, error) {
	column := &Column{
		schema: record.Field{
			Type: fieldType,
			Name: name,
		},
		cv: record.ColVal{},
	}
	column.cv.Init()
	if err := t.appendFieldNulls(column, t.RowCount); err != nil {
		return nil, err
	}

	return column, nil
}

func (t *Transform) appendFieldNulls(column *Column, count int) error {
	switch column.schema.Type {
	case influx.Field_Type_Tag, influx.Field_Type_String:
		column.cv.AppendStringNulls(count)
		return nil
	case influx.Field_Type_Int, influx.Field_Type_UInt:
		column.cv.AppendIntegerNulls(count)
		return nil
	case influx.Field_Type_Boolean:
		column.cv.AppendBooleanNulls(count)
		return nil
	case influx.Field_Type_Float:
		column.cv.AppendFloatNulls(count)
		return nil
	default:
		return ErrInvalidFieldType
	}
}

// getFieldType returns the corresponding Field type based on the field value
func (t *Transform) getFieldType(v interface{}) (int, error) {
	switch v.(type) {
	case string:
		return influx.Field_Type_String, nil
	case bool:
		return influx.Field_Type_Boolean, nil
	case float64, float32:
		return influx.Field_Type_Float, nil
	case int, int64, int32, uint, uint32, uint64:
		return influx.Field_Type_Int, nil
	}
	return influx.Field_Type_Unknown, ErrUnknownFieldType
}

// appendFieldValue appends field value to the column
func (t *Transform) appendFieldValue(column *Column, value interface{}) error {
	switch v := value.(type) {
	case string:
		column.cv.AppendString(v)
		return nil
	case bool:
		column.cv.AppendBoolean(v)
		return nil
	case float64:
		column.cv.AppendFloat(v)
		return nil
	case float32:
		column.cv.AppendFloat(float64(v))
		return nil
	case int:
		column.cv.AppendInteger(int64(v))
		return nil
	case int64:
		column.cv.AppendInteger(v)
		return nil
	case int32:
		column.cv.AppendInteger(int64(v))
		return nil
	case uint:
		column.cv.AppendInteger(int64(v))
		return nil
	case uint32:
		column.cv.AppendInteger(int64(v))
		return nil
	case uint64:
		column.cv.AppendInteger(int64(v))
		return nil
	}
	// For unknown types, try to throw error
	return ErrUnknownFieldType
}

func (t *Transform) processTagColumns(tags map[string]string) (err error) {
	for tagName, tagValue := range tags {
		if err := validateName(tagName); err != nil {
			return err
		}
		tagColumn, ok := t.Columns[tagName]
		if !ok {
			tagColumn, err = t.createColumn(tagName, influx.Field_Type_Tag)
			if err != nil {
				return err
			}
		}
		// write the tag value to column
		tagColumn.cv.AppendString(tagValue)
		t.fillChecker[tagName] = true
		t.Columns[tagName] = tagColumn
	}
	return nil
}

func (t *Transform) processFieldColumns(fields map[string]interface{}) (err error) {
	for fieldName, fieldValue := range fields {
		if err := validateName(fieldName); err != nil {
			return err
		}
		fieldType, err := t.getFieldType(fieldValue)
		if err != nil {
			return err
		}
		fieldColumn, ok := t.Columns[fieldName]
		if !ok {
			fieldColumn, err = t.createColumn(fieldName, fieldType)
			if err != nil {
				return err
			}
		}

		if err := t.appendFieldValue(fieldColumn, fieldValue); err != nil {
			return err
		}

		t.fillChecker[fieldName] = true
		t.Columns[fieldName] = fieldColumn
	}
	return nil
}

// processTimestamp handles timestamp processing with validation
func (t *Transform) processTimestamp(tt time.Time) (err error) {
	var timestamp = time.Now().UnixNano()
	if !tt.IsZero() {
		timestamp = tt.UnixNano()
	}

	timeCol, exists := t.Columns[TimeColumnName]
	if !exists {
		timeCol, err = t.createColumn(TimeColumnName, influx.Field_Type_Int)
		if err != nil {
			return err
		}
	}

	timeCol.cv.AppendInteger(timestamp)
	t.Columns[TimeColumnName] = timeCol

	// Update min/max time
	if timestamp < t.MinTime {
		t.MinTime = timestamp
	}
	if timestamp > t.MaxTime {
		t.MaxTime = timestamp
	}
	return nil
}

func (t *Transform) processMissValueColumns() error {
	for fieldName, ok := range t.fillChecker {
		if ok {
			continue
		}
		column, ok := t.Columns[fieldName]
		if !ok {
			continue
		}
		offset := t.RowCount - column.cv.Len
		if offset == 0 {
			continue
		}
		if err := t.appendFieldNulls(column, offset); err != nil {
			return err
		}
	}
	t.resetFillChecker()
	return nil
}

// validateName checks if the column name is valid
func validateName(name string) error {
	if name == "" {
		return ErrEmptyName
	}
	if name == TimeColumnName {
		return ErrInvalidTimeColumn
	}
	return nil
}

// ToSrvRecords converts to record.Record with improved sorting and validation
func (t *Transform) ToSrvRecords() (*record.Record, error) {
	t.mux.RLock()
	defer t.mux.RUnlock()

	if len(t.Columns) == 0 {
		return nil, errors.New("no columns to convert")
	}

	rec := &record.Record{}
	rec.Schema = make([]record.Field, 0, len(t.Columns))
	rec.ColVals = make([]record.ColVal, 0, len(t.Columns))

	for _, column := range t.Columns {
		rec.Schema = append(rec.Schema, column.schema)
		rec.ColVals = append(rec.ColVals, column.cv)
	}

	// Sort and validate the record
	sort.Sort(rec)
	record.CheckRecord(rec)
	rec = record.NewColumnSortHelper().Sort(rec)

	return rec, nil
}

// resetFillChecker clears fill checker map
func (t *Transform) resetFillChecker() {
	for key := range t.fillChecker {
		t.fillChecker[key] = false
	}
}
