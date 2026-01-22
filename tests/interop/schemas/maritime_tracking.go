package schemas

import "fmt"

// MaritimeTrackingSchema models a maritime vessel tracking system.
// Tests deep nesting (3-4 levels) and complex relationship patterns.
//
// Hierarchy:
//   - Vessel -> Voyage -> PortCall -> Event
//   - Each level adds depth for testing nested query resolution
const MaritimeTrackingSchema = `
type Vessel {
	imo: String
	name: String
	flag: String
	vesselType: String
	grossTonnage: Int
	deadweight: Int
	yearBuilt: Int
	voyages: [Voyage] @relation
}

type Voyage {
	voyageNumber: String
	status: String
	departurePort: String
	arrivalPort: String
	departureDate: DateTime
	arrivalDate: DateTime
	cargoType: String
	cargoWeight: Float
	vessel: Vessel @relation
	portCalls: [PortCall] @relation
}

type PortCall {
	portCode: String
	portName: String
	country: String
	arrivalTime: DateTime
	departureTime: DateTime
	berthNumber: String
	pilotRequired: Boolean
	voyage: Voyage @relation
	events: [PortEvent] @relation
}

type PortEvent {
	eventType: String
	timestamp: DateTime
	description: String
	latitude: Float
	longitude: Float
	reportedBy: String
	portCall: PortCall @relation
}
`

// MaritimeCollectionNames contains collection names for maritime schema.
var MaritimeCollectionNames = struct {
	Vessel    string
	Voyage    string
	PortCall  string
	PortEvent string
}{
	Vessel:    "Vessel",
	Voyage:    "Voyage",
	PortCall:  "PortCall",
	PortEvent: "PortEvent",
}

// VesselDoc represents input data for creating a Vessel document.
type VesselDoc struct {
	IMO          string
	Name         string
	Flag         *string
	VesselType   *string
	GrossTonnage *int
	Deadweight   *int
	YearBuilt    *int
}

// VoyageDoc represents input data for creating a Voyage document.
type VoyageDoc struct {
	VoyageNumber  string
	Status        *string
	DeparturePort *string
	ArrivalPort   *string
	DepartureDate *string
	ArrivalDate   *string
	CargoType     *string
	CargoWeight   *float64
	VesselID      string // Document ID of the related Vessel
}

// PortCallDoc represents input data for creating a PortCall document.
type PortCallDoc struct {
	PortCode      string
	PortName      *string
	Country       *string
	ArrivalTime   *string
	DepartureTime *string
	BerthNumber   *string
	PilotRequired *bool
	VoyageID      string // Document ID of the related Voyage
}

// PortEventDoc represents input data for creating a PortEvent document.
type PortEventDoc struct {
	EventType   string
	Timestamp   string
	Description *string
	Latitude    *float64
	Longitude   *float64
	ReportedBy  *string
	PortCallID  string // Document ID of the related PortCall
}

// CreateVessel generates a GraphQL mutation to create a Vessel document.
func CreateVessel(doc VesselDoc) string {
	return fmt.Sprintf(`mutation {
		create_Vessel(input: {
			imo: %q
			name: %q
			flag: %s
			vesselType: %s
			grossTonnage: %s
			deadweight: %s
			yearBuilt: %s
		}) {
			_docID
			imo
			name
		}
	}`,
		doc.IMO,
		doc.Name,
		nullableString(doc.Flag),
		nullableString(doc.VesselType),
		nullableInt(doc.GrossTonnage),
		nullableInt(doc.Deadweight),
		nullableInt(doc.YearBuilt),
	)
}

// CreateVoyage generates a GraphQL mutation to create a Voyage document.
func CreateVoyage(doc VoyageDoc) string {
	return fmt.Sprintf(`mutation {
		create_Voyage(input: {
			voyageNumber: %q
			status: %s
			departurePort: %s
			arrivalPort: %s
			departureDate: %s
			arrivalDate: %s
			cargoType: %s
			cargoWeight: %s
			vessel_id: %q
		}) {
			_docID
			voyageNumber
		}
	}`,
		doc.VoyageNumber,
		nullableString(doc.Status),
		nullableString(doc.DeparturePort),
		nullableString(doc.ArrivalPort),
		nullableString(doc.DepartureDate),
		nullableString(doc.ArrivalDate),
		nullableString(doc.CargoType),
		nullableFloat(doc.CargoWeight),
		doc.VesselID,
	)
}

// CreatePortCall generates a GraphQL mutation to create a PortCall document.
func CreatePortCall(doc PortCallDoc) string {
	return fmt.Sprintf(`mutation {
		create_PortCall(input: {
			portCode: %q
			portName: %s
			country: %s
			arrivalTime: %s
			departureTime: %s
			berthNumber: %s
			pilotRequired: %s
			voyage_id: %q
		}) {
			_docID
			portCode
		}
	}`,
		doc.PortCode,
		nullableString(doc.PortName),
		nullableString(doc.Country),
		nullableString(doc.ArrivalTime),
		nullableString(doc.DepartureTime),
		nullableString(doc.BerthNumber),
		nullableBool(doc.PilotRequired),
		doc.VoyageID,
	)
}

// CreatePortEvent generates a GraphQL mutation to create a PortEvent document.
func CreatePortEvent(doc PortEventDoc) string {
	return fmt.Sprintf(`mutation {
		create_PortEvent(input: {
			eventType: %q
			timestamp: %q
			description: %s
			latitude: %s
			longitude: %s
			reportedBy: %s
			portCall_id: %q
		}) {
			_docID
			eventType
			timestamp
		}
	}`,
		doc.EventType,
		doc.Timestamp,
		nullableString(doc.Description),
		nullableFloat(doc.Latitude),
		nullableFloat(doc.Longitude),
		nullableString(doc.ReportedBy),
		doc.PortCallID,
	)
}

// QueryVesselsFull generates a deep nested query (4 levels).
func QueryVesselsFull() string {
	return `{
		Vessel {
			_docID
			imo
			name
			flag
			vesselType
			grossTonnage
			deadweight
			yearBuilt
			voyages {
				_docID
				voyageNumber
				status
				departurePort
				arrivalPort
				departureDate
				arrivalDate
				cargoType
				cargoWeight
				portCalls {
					_docID
					portCode
					portName
					country
					arrivalTime
					departureTime
					berthNumber
					pilotRequired
					events {
						_docID
						eventType
						timestamp
						description
						latitude
						longitude
						reportedBy
					}
				}
			}
		}
	}`
}

// QueryVoyagesByStatus generates a query filtering voyages by status.
func QueryVoyagesByStatus(status string) string {
	return fmt.Sprintf(`{
		Voyage(filter: {status: {_eq: %q}}) {
			_docID
			voyageNumber
			status
			departurePort
			arrivalPort
			vessel {
				imo
				name
			}
			portCalls {
				portCode
				portName
			}
		}
	}`, status)
}

// QueryPortCallsWithEvents generates a query for port calls with their events.
func QueryPortCallsWithEvents() string {
	return `{
		PortCall {
			_docID
			portCode
			portName
			country
			arrivalTime
			departureTime
			voyage {
				voyageNumber
				vessel {
					imo
					name
				}
			}
			events {
				eventType
				timestamp
				description
			}
		}
	}`
}

// QueryEventsByType generates a query filtering events by type.
func QueryEventsByType(eventType string) string {
	return fmt.Sprintf(`{
		PortEvent(filter: {eventType: {_eq: %q}}) {
			_docID
			eventType
			timestamp
			description
			latitude
			longitude
			portCall {
				portCode
				portName
				voyage {
					voyageNumber
					vessel {
						name
						imo
					}
				}
			}
		}
	}`, eventType)
}
