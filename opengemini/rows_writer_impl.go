package opengemini

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc/keepalive"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/opengemini-client-go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	Field_Type_Unknown = 0
	Field_Type_Int     = 1
	Field_Type_UInt    = 2
	Field_Type_Float   = 3
	Field_Type_String  = 4
	Field_Type_Boolean = 5
	Field_Type_Tag     = 6
	Field_Type_Last    = 7
)

type RowsWriterClient struct {
	cfg        *Config
	mux        sync.RWMutex
	transforms map[string]transform
}

func (r *RowsWriterClient) Ping(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (r *RowsWriterClient) WritePoint(ctx context.Context, database, retentionPolicy string, point *Point) error {
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

func (r *RowsWriterClient) WritePoints(ctx context.Context, database, retentionPolicy string, points []*Point) error {
	for _, point := range points {
		if err := r.WritePoint(ctx, database, retentionPolicy, point); err != nil {
			return err
		}
	}
	return nil
}

func (r *RowsWriterClient) Flush(ctx context.Context, database, retentionPolicy string) error {
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

	// 配置gRPC连接参数
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// 启用keepalive以保持长连接
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second, // 每10秒ping一次
			Timeout:             3 * time.Second,  // 3秒超时
			PermitWithoutStream: true,             // 允许无数据时保持连接
		}),
		// 配置初始窗口大小和连接窗口大小
		grpc.WithInitialWindowSize(1 << 24),     // 16MB
		grpc.WithInitialConnWindowSize(1 << 24), // 16MB
		// 启用压缩?
		grpc.WithDefaultCallOptions(grpc.UseCompressor("gzip")),
		// 配置读写最大消息大小
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64 * 1024 * 1024)), // 64MB
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(64 * 1024 * 1024)), // 64MB
	}

	conn, err := grpc.NewClient("127.0.0.1:8080", opts...)
	if err != nil {
		return fmt.Errorf("failed to connect: %v", err)
	}
	defer conn.Close()

	client := proto.NewWriteRowsServiceClient(conn)
	var req = &proto.WriteRowsRequest{
		Version:  0,
		Database: database,
		Rp:       retentionPolicy,
		Username: "admin",
		Password: "Admin@123",
	}

	// 使用带超时的上下文
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	for mst, rawRecord := range t {
		rec, err := rawRecord.ToSrvRecords()
		if err != nil {
			return fmt.Errorf("failed to convert records: %v", err)
		}
		var buff []byte
		buff, err = rec.Marshal(buff)
		if err != nil {
			return fmt.Errorf("failed to marshal record: %v", err)
		}

		req.Rows = append(req.Rows, &proto.Row{
			Measurement:  mst,
			MinTime:      rawRecord.MinTime,
			MaxTime:      rawRecord.MaxTime,
			CompressAlgo: 0,
			Block:        buff,
		})
	}

	response, err := client.WriteRows(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to write rows: %v", err)
	}

	fmt.Println(response)

	return nil
}

func (r *RowsWriterClient) Close() error {
	//TODO implement me
	panic("implement me")
}

func NewRPCClient(cfg *Config) RPCClient {
	return &RowsWriterClient{
		cfg:        cfg,
		transforms: make(map[string]transform),
	}
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
		c = &columner{}
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
	case Field_Type_Tag, Field_Type_String:
		column.cv.AppendStringNulls(count)
		return nil
	case Field_Type_Int, Field_Type_UInt:
		column.cv.AppendIntegerNulls(count)
		return nil
	case Field_Type_Boolean:
		column.cv.AppendBooleanNulls(count)
		return nil
	case Field_Type_Float:
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
		return Field_Type_String, nil
	case bool:
		return Field_Type_Boolean, nil
	case float64, float32:
		return Field_Type_Float, nil
	case int, int64, int32, uint, uint32, uint64:
		return Field_Type_Int, nil
	}
	return Field_Type_Unknown, ErrUnknownFieldType
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
			tagColumn, err = c.createColumn(tagName, Field_Type_Tag)
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

	timeCol, exists := c.Columns[TimeColumnName]
	if !exists {
		timeCol, err = c.createColumn(TimeColumnName, Field_Type_Int)
		if err != nil {
			return err
		}
	}

	timeCol.cv.AppendInteger(timestamp)
	c.Columns[TimeColumnName] = timeCol

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
	if name == TimeColumnName {
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
