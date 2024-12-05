package opengemini

var _ RecordBuilder = (*recordBuilder)(nil)

type RecordBuilder interface {
	Database(database string) RecordBuilder
	RetentionPolicy(policy string) RecordBuilder
	Measurement(measurement string) RecordBuilder
	Build() *recordBuilder
}

type recordBuilder struct {
	database        string
	retentionPolicy string
	measurement     string
}

func (r *recordBuilder) Database(database string) RecordBuilder {
	r.database = database
	return r
}

func (r *recordBuilder) RetentionPolicy(policy string) RecordBuilder {
	r.retentionPolicy = policy
	return r
}

func (r *recordBuilder) Measurement(measurement string) RecordBuilder {
	r.measurement = measurement
	return r
}

func (r *recordBuilder) Build() *recordBuilder {
	return r
}
