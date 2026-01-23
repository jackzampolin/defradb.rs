package schemas

import "fmt"

// RetailPOSSchema models a retail point-of-sale system.
// Tests transactions with line items, a common pattern for financial systems.
//
// Hierarchy:
//   - Store -> Transaction -> LineItem
//   - Products are referenced by LineItems
//   - Supports discounts and tax calculations
const RetailPOSSchema = `
type Store {
	storeCode: String
	name: String
	address: String
	city: String
	region: String
	postalCode: String
	phone: String
	isActive: Boolean
	transactions: [Transaction] @relation
}

type Product {
	sku: String
	name: String
	description: String
	category: String
	basePrice: Float
	taxRate: Float
	isAvailable: Boolean
	lineItems: [LineItem] @relation
}

type Transaction {
	transactionId: String
	timestamp: DateTime
	subtotal: Float
	taxAmount: Float
	discountAmount: Float
	total: Float
	paymentMethod: String
	customerId: String
	cashierId: String
	status: String
	store: Store @relation
	lineItems: [LineItem] @relation
}

type LineItem {
	quantity: Int
	unitPrice: Float
	discount: Float
	lineTotal: Float
	notes: String
	transaction: Transaction @relation
	product: Product @relation
}
`

// RetailCollectionNames contains collection names for retail schema.
var RetailCollectionNames = struct {
	Store       string
	Product     string
	Transaction string
	LineItem    string
}{
	Store:       "Store",
	Product:     "Product",
	Transaction: "Transaction",
	LineItem:    "LineItem",
}

// StoreDoc represents input data for creating a Store document.
type StoreDoc struct {
	StoreCode  string
	Name       string
	Address    *string
	City       *string
	Region     *string
	PostalCode *string
	Phone      *string
	IsActive   *bool
}

// ProductDoc represents input data for creating a Product document.
type ProductDoc struct {
	SKU         string
	Name        string
	Description *string
	Category    *string
	BasePrice   float64
	TaxRate     *float64
	IsAvailable *bool
}

// TransactionDoc represents input data for creating a Transaction document.
type TransactionDoc struct {
	TransactionID  string
	Timestamp      string
	Subtotal       float64
	TaxAmount      *float64
	DiscountAmount *float64
	Total          float64
	PaymentMethod  *string
	CustomerID     *string
	CashierID      *string
	Status         *string
	StoreID        string // Document ID of the related Store
}

// LineItemDoc represents input data for creating a LineItem document.
type LineItemDoc struct {
	Quantity      int
	UnitPrice     float64
	Discount      *float64
	LineTotal     float64
	Notes         *string
	TransactionID string // Document ID of the related Transaction
	ProductID     string // Document ID of the related Product
}

// CreateStore generates a GraphQL mutation to create a Store document.
func CreateStore(doc StoreDoc) string {
	return fmt.Sprintf(`mutation {
		create_Store(input: {
			storeCode: %q
			name: %q
			address: %s
			city: %s
			region: %s
			postalCode: %s
			phone: %s
			isActive: %s
		}) {
			_docID
			storeCode
			name
		}
	}`,
		doc.StoreCode,
		doc.Name,
		nullableString(doc.Address),
		nullableString(doc.City),
		nullableString(doc.Region),
		nullableString(doc.PostalCode),
		nullableString(doc.Phone),
		nullableBool(doc.IsActive),
	)
}

// CreateProduct generates a GraphQL mutation to create a Product document.
func CreateProduct(doc ProductDoc) string {
	return fmt.Sprintf(`mutation {
		create_Product(input: {
			sku: %q
			name: %q
			description: %s
			category: %s
			basePrice: %f
			taxRate: %s
			isAvailable: %s
		}) {
			_docID
			sku
			name
		}
	}`,
		doc.SKU,
		doc.Name,
		nullableString(doc.Description),
		nullableString(doc.Category),
		doc.BasePrice,
		nullableFloat(doc.TaxRate),
		nullableBool(doc.IsAvailable),
	)
}

// CreateTransaction generates a GraphQL mutation to create a Transaction document.
func CreateTransaction(doc TransactionDoc) string {
	return fmt.Sprintf(`mutation {
		create_Transaction(input: {
			transactionId: %q
			timestamp: %q
			subtotal: %f
			taxAmount: %s
			discountAmount: %s
			total: %f
			paymentMethod: %s
			customerId: %s
			cashierId: %s
			status: %s
			store_id: %q
		}) {
			_docID
			transactionId
		}
	}`,
		doc.TransactionID,
		doc.Timestamp,
		doc.Subtotal,
		nullableFloat(doc.TaxAmount),
		nullableFloat(doc.DiscountAmount),
		doc.Total,
		nullableString(doc.PaymentMethod),
		nullableString(doc.CustomerID),
		nullableString(doc.CashierID),
		nullableString(doc.Status),
		doc.StoreID,
	)
}

// CreateLineItem generates a GraphQL mutation to create a LineItem document.
func CreateLineItem(doc LineItemDoc) string {
	return fmt.Sprintf(`mutation {
		create_LineItem(input: {
			quantity: %d
			unitPrice: %f
			discount: %s
			lineTotal: %f
			notes: %s
			transaction_id: %q
			product_id: %q
		}) {
			_docID
			quantity
			lineTotal
		}
	}`,
		doc.Quantity,
		doc.UnitPrice,
		nullableFloat(doc.Discount),
		doc.LineTotal,
		nullableString(doc.Notes),
		doc.TransactionID,
		doc.ProductID,
	)
}

// QueryTransactionsFull generates a query for transactions with all related data.
func QueryTransactionsFull() string {
	return `{
		Transaction {
			_docID
			transactionId
			timestamp
			subtotal
			taxAmount
			discountAmount
			total
			paymentMethod
			customerId
			cashierId
			status
			store {
				_docID
				storeCode
				name
				city
			}
			lineItems {
				_docID
				quantity
				unitPrice
				discount
				lineTotal
				product {
					_docID
					sku
					name
					category
					basePrice
				}
			}
		}
	}`
}

// QueryStoreTransactions generates a query for a store's transactions.
func QueryStoreTransactions(storeCode string) string {
	return fmt.Sprintf(`{
		Store(filter: {storeCode: {_eq: %q}}) {
			_docID
			storeCode
			name
			transactions {
				transactionId
				timestamp
				total
				status
				lineItems {
					quantity
					lineTotal
					product {
						name
						sku
					}
				}
			}
		}
	}`, storeCode)
}

// QueryProductSales generates a query for a product's sales across transactions.
func QueryProductSales(sku string) string {
	return fmt.Sprintf(`{
		Product(filter: {sku: {_eq: %q}}) {
			_docID
			sku
			name
			basePrice
			lineItems {
				quantity
				unitPrice
				lineTotal
				transaction {
					transactionId
					timestamp
					store {
						storeCode
						name
					}
				}
			}
		}
	}`, sku)
}

// QueryTransactionsByStatus generates a query for transactions by status.
func QueryTransactionsByStatus(status string) string {
	return fmt.Sprintf(`{
		Transaction(filter: {status: {_eq: %q}}) {
			_docID
			transactionId
			timestamp
			total
			paymentMethod
			store {
				name
				storeCode
			}
		}
	}`, status)
}
