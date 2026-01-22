package schemas

import "fmt"

// IoTSensorSchema models an IoT deployment with devices and sensor readings.
// Tests nested relationships and time-series data patterns.
//
// Hierarchy:
//   - Device (1:N) -> SensorReading
//   - Each device has metadata and a relation to its readings
//   - Readings have timestamps and numeric values
const IoTSensorSchema = `
type Device {
	deviceId: String
	name: String
	location: String
	deviceType: String
	isActive: Boolean
	firmware: String
	lastSeen: DateTime
	readings: [SensorReading] @relation
}

type SensorReading {
	timestamp: DateTime
	sensorType: String
	value: Float
	unit: String
	quality: Int
	device: Device @relation
}
`

// IoTCollectionNames contains the collection names for IoT schema.
var IoTCollectionNames = struct {
	Device        string
	SensorReading string
}{
	Device:        "Device",
	SensorReading: "SensorReading",
}

// DeviceDoc represents input data for creating a Device document.
type DeviceDoc struct {
	DeviceId   string
	Name       *string
	Location   *string
	DeviceType *string
	IsActive   *bool
	Firmware   *string
	LastSeen   *string
}

// SensorReadingDoc represents input data for creating a SensorReading document.
type SensorReadingDoc struct {
	Timestamp  string
	SensorType string
	Value      float64
	Unit       *string
	Quality    *int
	DeviceID   string // Document ID of the related Device
}

// CreateDevice generates a GraphQL mutation to create a Device document.
func CreateDevice(doc DeviceDoc) string {
	return fmt.Sprintf(`mutation {
		create_Device(input: {
			deviceId: %q
			name: %s
			location: %s
			deviceType: %s
			isActive: %s
			firmware: %s
			lastSeen: %s
		}) {
			_docID
			deviceId
		}
	}`,
		doc.DeviceId,
		nullableString(doc.Name),
		nullableString(doc.Location),
		nullableString(doc.DeviceType),
		nullableBool(doc.IsActive),
		nullableString(doc.Firmware),
		nullableString(doc.LastSeen),
	)
}

// CreateSensorReading generates a GraphQL mutation to create a SensorReading document.
func CreateSensorReading(doc SensorReadingDoc) string {
	return fmt.Sprintf(`mutation {
		create_SensorReading(input: {
			timestamp: %q
			sensorType: %q
			value: %f
			unit: %s
			quality: %s
			device_id: %q
		}) {
			_docID
			timestamp
			sensorType
			value
		}
	}`,
		doc.Timestamp,
		doc.SensorType,
		doc.Value,
		nullableString(doc.Unit),
		nullableInt(doc.Quality),
		doc.DeviceID,
	)
}

// QueryDevicesWithReadings generates a GraphQL query to fetch devices with their readings.
func QueryDevicesWithReadings() string {
	return `{
		Device {
			_docID
			deviceId
			name
			location
			deviceType
			isActive
			firmware
			lastSeen
			readings {
				_docID
				timestamp
				sensorType
				value
				unit
				quality
			}
		}
	}`
}

// QuerySensorReadings generates a GraphQL query to fetch sensor readings with device info.
func QuerySensorReadings() string {
	return `{
		SensorReading {
			_docID
			timestamp
			sensorType
			value
			unit
			quality
			device {
				_docID
				deviceId
				name
			}
		}
	}`
}

// QueryDevicesBySensorType generates a query to find devices that have readings of a specific type.
func QueryDevicesBySensorType(sensorType string) string {
	return fmt.Sprintf(`{
		Device(filter: {readings: {sensorType: {_eq: %q}}}) {
			_docID
			deviceId
			name
			readings(filter: {sensorType: {_eq: %q}}) {
				timestamp
				value
				unit
			}
		}
	}`, sensorType, sensorType)
}

// QueryReadingsInRange generates a query for readings with values in a range.
func QueryReadingsInRange(minValue, maxValue float64) string {
	return fmt.Sprintf(`{
		SensorReading(filter: {value: {_gte: %f, _lte: %f}}) {
			_docID
			timestamp
			sensorType
			value
			device {
				deviceId
				name
			}
		}
	}`, minValue, maxValue)
}
