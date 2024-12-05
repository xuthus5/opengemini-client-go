package opengemini

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/openGemini/opengemini-client-go/lib/record"
	"github.com/openGemini/opengemini-client-go/proto"
)

func (c *client) WriteByGRPC(ctx context.Context, rbs ...RecordBuilder) error {
	return c.rpcClient.writeRecords(ctx, rbs...)
}

type recordWriterClient struct {
	cfg        *RPCConfig
	mux        sync.RWMutex
	lb         *rpcLoadBalance
	transforms map[string]transform
}

func newRecordWriterClient(cfg *RPCConfig) (*recordWriterClient, error) {
	if len(cfg.Addresses) == 0 {
		return nil, ErrEmptyAddress
	}

	balance, err := newRPCLoadBalance(cfg)
	if err != nil {
		return nil, errors.New("create rpc load balance failed: " + err.Error())
	}

	rw := &recordWriterClient{transforms: make(map[string]transform), lb: balance}
	return rw, nil
}

func (r *recordWriterClient) writeRecord(ctx context.Context, rb RecordBuilder) error {
	if err := checkDatabaseAndPolicy(database, retentionPolicy); err != nil {
		return err
	}
	r.mux.Lock()
	defer r.mux.Unlock()
	name := database + retentionPolicy
	transform, ok := r.transforms[name]
	if !ok {
		transform = newTransform()
	}

	if err := transform.AppendPoint(point); err != nil {
		return err
	}

	r.transforms[name] = transform
	return nil
}

func (r *recordWriterClient) writeRecords(ctx context.Context, rbs ...RecordBuilder) error {
	for _, rb := range rbs {
		if err := r.writeRecord(ctx, rb); err != nil {
			return err
		}
	}
	return nil
}

func (r *recordWriterClient) Flush(ctx context.Context, database, retentionPolicy string) (err error) {
	if err := checkDatabaseAndPolicy(database, retentionPolicy); err != nil {
		return err
	}

	r.mux.Lock()
	defer r.mux.Unlock()

	name := database + retentionPolicy
	t, ok := r.transforms[name]
	if !ok {
		return ErrEmptyRecord
	}

	if len(t) == 0 {
		return ErrEmptyRecord
	}

	fmt.Println("record: ", len(t))

	var req = &proto.WriteRequest{
		Database:        database,
		RetentionPolicy: retentionPolicy,
	}

	if r.cfg.AuthConfig != nil {
		req.Username = r.cfg.AuthConfig.Username
		req.Password = r.cfg.AuthConfig.Password
	}

	// 使用带超时的上下文
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	for mst, rawRecord := range t {
		rec, err := rawRecord.ToSrvRecords()
		if err != nil {
			return fmt.Errorf("failed to convert records: %v", err)
		}
		fmt.Println(rec.String())
		var buff []byte
		buff, err = rec.Marshal(buff)
		if err != nil {
			return fmt.Errorf("failed to marshal record: %v", err)
		}

		req.Records = append(req.Records, &proto.Record{
			Measurement: mst,
			MinTime:     rawRecord.MinTime,
			MaxTime:     rawRecord.MaxTime,
			Block:       buff,
		})
	}

	response, err := r.lb.Next().Write(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to write rows: %v", err)
	}

	t.reset()

	fmt.Println("response code: ", response.Code)

	return nil
}

func (r *recordWriterClient) Close() error {
	return nil
}

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

type columner struct {
	RowCount    int
	MinTime     int64
	MaxTime     int64
	Columns     map[string]*Column
	fillChecker map[string]bool
}

type transform map[string]*columner

// newTransform creates a new transform instance with configuration
func newTransform() transform {
	return make(transform)
}

// AppendPoint writes data by row with improved error handling
func (t *transform) AppendPoint(point *Point) error {
	if err := checkMeasurementName(point.Measurement); err != nil {
		return err
	}

	c, ok := (*t)[point.Measurement]
	if !ok {
		c = &columner{
			Columns:     make(map[string]*Column),
			fillChecker: make(map[string]bool),
		}
	}

	// process tags
	if err := c.processTagColumns(point.Tags); err != nil {
		return err
	}

	// process fields
	if err := c.processFieldColumns(point.Fields); err != nil {
		return err
	}

	// process timestamp
	if err := c.processTimestamp(point.Time); err != nil {
		return err
	}

	c.RowCount++

	// fill another field or tag
	if err := c.processMissValueColumns(); err != nil {
		return err
	}

	(*t)[point.Measurement] = c

	return nil
}

func (t *transform) AppendPoints(points []*Point) (failedParts []*Point, err error) {
	for _, point := range points {
		if failedErr := t.AppendPoint(point); failedErr != nil {
			failedParts = append(failedParts, point)
			err = errors.Join(err, failedErr)
		}
	}
	return failedParts, err
}

func (t *transform) reset() {
	for k := range *t {
		delete(*t, k)
	}
}

func (c *columner) createColumn(name string, fieldType int) (*Column, error) {
	column := &Column{
		schema: record.Field{
			Type: fieldType,
			Name: name,
		},
		cv: record.ColVal{},
	}
	column.cv.Init()
	if err := c.appendFieldNulls(column, c.RowCount); err != nil {
		return nil, err
	}

	return column, nil
}

func (c *columner) appendFieldNulls(column *Column, count int) error {
	switch column.schema.Type {
	case record.FieldTypeTag, record.FieldTypeString:
		column.cv.AppendStringNulls(count)
		return nil
	case record.FieldTypeInt, record.FieldTypeUInt:
		column.cv.AppendIntegerNulls(count)
		return nil
	case record.FieldTypeBoolean:
		column.cv.AppendBooleanNulls(count)
		return nil
	case record.FieldTypeFloat:
		column.cv.AppendFloatNulls(count)
		return nil
	default:
		return ErrInvalidFieldType
	}
}

// getFieldType returns the corresponding Field type based on the field value
func (c *columner) getFieldType(v interface{}) (int, error) {
	switch v.(type) {
	case string:
		return record.FieldTypeString, nil
	case bool:
		return record.FieldTypeBoolean, nil
	case float64, float32:
		return record.FieldTypeFloat, nil
	case int, int64, int32, uint, uint32, uint64:
		return record.FieldTypeInt, nil
	}
	return record.FieldTypeUnknown, ErrUnknownFieldType
}

// appendFieldValue appends field value to the column
func (c *columner) appendFieldValue(column *Column, value interface{}) error {
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

func (c *columner) processTagColumns(tags map[string]string) (err error) {
	for tagName, tagValue := range tags {
		if err := validateName(tagName); err != nil {
			return err
		}
		tagColumn, ok := c.Columns[tagName]
		if !ok {
			tagColumn, err = c.createColumn(tagName, record.FieldTypeTag)
			if err != nil {
				return err
			}
		}
		// write the tag value to column
		tagColumn.cv.AppendString(tagValue)
		c.fillChecker[tagName] = true
		c.Columns[tagName] = tagColumn
	}
	return nil
}

func (c *columner) processFieldColumns(fields map[string]interface{}) (err error) {
	for fieldName, fieldValue := range fields {
		if err := validateName(fieldName); err != nil {
			return err
		}
		fieldType, err := c.getFieldType(fieldValue)
		if err != nil {
			return err
		}
		fieldColumn, ok := c.Columns[fieldName]
		if !ok {
			fieldColumn, err = c.createColumn(fieldName, fieldType)
			if err != nil {
				return err
			}
		}

		if err := c.appendFieldValue(fieldColumn, fieldValue); err != nil {
			return err
		}

		c.fillChecker[fieldName] = true
		c.Columns[fieldName] = fieldColumn
	}
	return nil
}

// processTimestamp handles timestamp processing with validation
func (c *columner) processTimestamp(tt time.Time) (err error) {
	var timestamp = time.Now().UnixNano()
	if !tt.IsZero() {
		timestamp = tt.UnixNano()
	}

	timeCol, exists := c.Columns[record.TimeField]
	if !exists {
		timeCol, err = c.createColumn(record.TimeField, record.FieldTypeInt)
		if err != nil {
			return err
		}
	}

	timeCol.cv.AppendInteger(timestamp)
	c.Columns[record.TimeField] = timeCol

	// Update min/max time
	if timestamp < c.MinTime {
		c.MinTime = timestamp
	}
	if timestamp > c.MaxTime {
		c.MaxTime = timestamp
	}
	return nil
}

func (c *columner) processMissValueColumns() error {
	for fieldName, ok := range c.fillChecker {
		if ok {
			continue
		}
		column, ok := c.Columns[fieldName]
		if !ok {
			continue
		}
		offset := c.RowCount - column.cv.Len
		if offset == 0 {
			continue
		}
		if err := c.appendFieldNulls(column, offset); err != nil {
			return err
		}
	}
	c.resetFillChecker()
	return nil
}

// validateName checks if the column name is valid
func validateName(name string) error {
	if name == "" {
		return ErrEmptyName
	}
	if name == record.TimeField {
		return ErrInvalidTimeColumn
	}
	return nil
}

// ToSrvRecords converts to record.Record with improved sorting and validation
func (c *columner) ToSrvRecords() (*record.Record, error) {
	if len(c.Columns) == 0 {
		return nil, ErrEmptyRecord
	}

	rec := &record.Record{}
	rec.Schema = make([]record.Field, 0, len(c.Columns))
	rec.ColVals = make([]record.ColVal, 0, len(c.Columns))

	for _, column := range c.Columns {
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
func (c *columner) resetFillChecker() {
	for key := range c.fillChecker {
		c.fillChecker[key] = false
	}
}
