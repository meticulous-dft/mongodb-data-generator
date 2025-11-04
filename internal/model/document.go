package model

import (
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// DocumentSize represents the target document size
type DocumentSize int

const (
	Size2KB  DocumentSize = 2 * 1024
	Size4KB  DocumentSize = 4 * 1024
	Size8KB  DocumentSize = 8 * 1024
	Size16KB DocumentSize = 16 * 1024
	Size32KB DocumentSize = 32 * 1024
	Size64KB DocumentSize = 64 * 1024
)

// CustomerDocument represents a customer with nested orders and details
type CustomerDocument struct {
	ID           primitive.ObjectID `bson:"_id"`
	CustomerID   string             `bson:"customer_id"`
	Email        string             `bson:"email"`
	FirstName    string             `bson:"first_name"`
	LastName     string             `bson:"last_name"`
	Phone        string             `bson:"phone"`
	DateOfBirth  time.Time          `bson:"date_of_birth"`
	CreatedAt    time.Time          `bson:"created_at"`
	UpdatedAt    time.Time          `bson:"updated_at"`
	
	Addresses    []Address          `bson:"addresses"`
	PaymentMethods []PaymentMethod  `bson:"payment_methods"`
	Orders       []Order            `bson:"orders"`
	
	// Metadata and padding fields
	Metadata     map[string]interface{} `bson:"metadata"`
	Notes        []string               `bson:"notes"`
	Tags         []string               `bson:"tags"`
	
	// Padding field to control document size
	Padding      string                 `bson:"padding"`
}

// Address represents a customer address
type Address struct {
	ID          primitive.ObjectID `bson:"_id"`
	Type        string             `bson:"type"` // home, work, shipping
	Street      string             `bson:"street"`
	City        string             `bson:"city"`
	State       string             `bson:"state"`
	ZipCode     string             `bson:"zip_code"`
	Country     string             `bson:"country"`
	IsDefault   bool               `bson:"is_default"`
	CreatedAt   time.Time          `bson:"created_at"`
}

// PaymentMethod represents a payment method
type PaymentMethod struct {
	ID          primitive.ObjectID `bson:"_id"`
	Type        string             `bson:"type"` // credit_card, debit_card, paypal
	CardNumber  string             `bson:"card_number"`
	CardHolder  string             `bson:"card_holder"`
	ExpiryMonth int                `bson:"expiry_month"`
	ExpiryYear  int                `bson:"expiry_year"`
	IsDefault   bool               `bson:"is_default"`
	CreatedAt   time.Time          `bson:"created_at"`
}

// Order represents an order with line items
type Order struct {
	ID          primitive.ObjectID `bson:"_id"`
	OrderNumber string             `bson:"order_number"`
	Status      string             `bson:"status"` // pending, processing, shipped, delivered, cancelled
	TotalAmount float64            `bson:"total_amount"`
	Currency    string             `bson:"currency"`
	OrderDate   time.Time          `bson:"order_date"`
	ShippedDate *time.Time         `bson:"shipped_date,omitempty"`
	DeliveredDate *time.Time       `bson:"delivered_date,omitempty"`
	
	ShippingAddress Address         `bson:"shipping_address"`
	BillingAddress  Address         `bson:"billing_address"`
	
	LineItems   []LineItem         `bson:"line_items"`
	
	Discounts   []Discount         `bson:"discounts"`
	Taxes       []Tax              `bson:"taxes"`
	
	Notes       string             `bson:"notes"`
	CreatedAt   time.Time          `bson:"created_at"`
	UpdatedAt   time.Time          `bson:"updated_at"`
}

// LineItem represents an order line item
type LineItem struct {
	ID          primitive.ObjectID `bson:"_id"`
	ProductID   string             `bson:"product_id"`
	ProductName string             `bson:"product_name"`
	SKU         string             `bson:"sku"`
	Quantity    int                `bson:"quantity"`
	UnitPrice   float64            `bson:"unit_price"`
	TotalPrice  float64            `bson:"total_price"`
	Category    string             `bson:"category"`
	Brand       string             `bson:"brand"`
	Description string             `bson:"description"`
}

// Discount represents an order discount
type Discount struct {
	ID          primitive.ObjectID `bson:"_id"`
	Type        string             `bson:"type"` // percentage, fixed
	Code        string             `bson:"code"`
	Amount      float64            `bson:"amount"`
	Description string             `bson:"description"`
}

// Tax represents a tax charge
type Tax struct {
	ID          primitive.ObjectID `bson:"_id"`
	Type        string             `bson:"type"` // sales, vat, shipping
	Rate        float64            `bson:"rate"`
	Amount      float64            `bson:"amount"`
	Description string             `bson:"description"`
}

// Generator generates customer documents with faker
type Generator struct {
	faker *gofakeit.Faker
	targetSize DocumentSize
	paddingTemplates map[DocumentSize]string
}

// NewGenerator creates a new document generator
func NewGenerator(targetSize DocumentSize) *Generator {
	faker := gofakeit.New(uint64(time.Now().UnixNano()))
	
	// Precompute padding templates for each size to avoid recomputation
	paddingTemplates := make(map[DocumentSize]string)
	sizes := []DocumentSize{Size2KB, Size4KB, Size8KB, Size16KB, Size32KB, Size64KB}
	
	for _, size := range sizes {
		// Generate a base document to measure, then calculate padding needed
		// We'll fine-tune this in the Generate method
		paddingTemplates[size] = ""
	}
	
	return &Generator{
		faker: faker,
		targetSize: targetSize,
		paddingTemplates: paddingTemplates,
	}
}

// TargetSize returns the target document size
func (g *Generator) TargetSize() DocumentSize {
	return g.targetSize
}

// Generate creates a new customer document with the target size
func (g *Generator) Generate() (*CustomerDocument, error) {
	now := time.Now()
	
	// Generate base customer data
	doc := &CustomerDocument{
		ID:          primitive.NewObjectID(),
		CustomerID:  g.faker.UUID(),
		Email:       g.faker.Email(),
		FirstName:   g.faker.FirstName(),
		LastName:    g.faker.LastName(),
		Phone:       g.faker.Phone(),
		DateOfBirth: g.faker.DateRange(time.Now().AddDate(-80, 0, 0), time.Now().AddDate(-18, 0, 0)),
		CreatedAt:   g.faker.DateRange(now.AddDate(-5, 0, 0), now),
		UpdatedAt:   now,
	}
	
	// Generate addresses (2-5 addresses)
	numAddresses := g.faker.IntRange(2, 6)
	doc.Addresses = make([]Address, numAddresses)
	for i := 0; i < numAddresses; i++ {
		doc.Addresses[i] = g.generateAddress(i == 0)
	}
	
	// Generate payment methods (1-4 payment methods)
	numPayments := g.faker.IntRange(1, 5)
	doc.PaymentMethods = make([]PaymentMethod, numPayments)
	for i := 0; i < numPayments; i++ {
		doc.PaymentMethods[i] = g.generatePaymentMethod(i == 0)
	}
	
	// Generate orders (variable number based on target size)
	// Larger documents need more orders to reach target size
	numOrders := g.calculateOrderCount()
	doc.Orders = make([]Order, numOrders)
	for i := 0; i < numOrders; i++ {
		doc.Orders[i] = g.generateOrder(now)
	}
	
	// Generate metadata
	doc.Metadata = g.generateMetadata()
	
	// Generate notes and tags
	numNotes := g.faker.IntRange(3, 8)
	doc.Notes = make([]string, numNotes)
	for i := 0; i < numNotes; i++ {
		doc.Notes[i] = g.faker.Paragraph(3, 5, 10, " ")
	}
	
	numTags := g.faker.IntRange(5, 15)
	doc.Tags = make([]string, numTags)
	for i := 0; i < numTags; i++ {
		doc.Tags[i] = g.faker.Word()
	}
	
	// Calculate and add padding to reach target size
	padding, err := g.calculatePadding(doc)
	if err != nil {
		return nil, err
	}
	doc.Padding = padding
	
	return doc, nil
}

// calculateOrderCount determines how many orders to generate based on target size
func (g *Generator) calculateOrderCount() int {
	// Base order count scales with document size
	baseCount := int(g.targetSize) / (8 * 1024) // Rough estimate
	if baseCount < 1 {
		baseCount = 1
	}
	if baseCount > 20 {
		baseCount = 20
	}
	
	// Add some variation
	return g.faker.IntRange(baseCount, baseCount+3)
}

// generateAddress creates a fake address
func (g *Generator) generateAddress(isDefault bool) Address {
	return Address{
		ID:        primitive.NewObjectID(),
		Type:      g.faker.RandomString([]string{"home", "work", "shipping", "billing"}),
		Street:    g.faker.Address().Address,
		City:      g.faker.City(),
		State:     g.faker.State(),
		ZipCode:   g.faker.Zip(),
		Country:   g.faker.Country(),
		IsDefault: isDefault,
		CreatedAt: g.faker.DateRange(time.Now().AddDate(-3, 0, 0), time.Now()),
	}
}

// generatePaymentMethod creates a fake payment method
func (g *Generator) generatePaymentMethod(isDefault bool) PaymentMethod {
	return PaymentMethod{
		ID:          primitive.NewObjectID(),
		Type:        g.faker.RandomString([]string{"credit_card", "debit_card", "paypal"}),
		CardNumber:  g.faker.CreditCard().Number,
		CardHolder:  g.faker.Name(),
		ExpiryMonth: g.faker.IntRange(1, 12),
		ExpiryYear:  g.faker.IntRange(2025, 2030),
		IsDefault:   isDefault,
		CreatedAt:   g.faker.DateRange(time.Now().AddDate(-2, 0, 0), time.Now()),
	}
}

// generateOrder creates a fake order with line items
func (g *Generator) generateOrder(baseTime time.Time) Order {
	orderDate := g.faker.DateRange(baseTime.AddDate(-2, 0, 0), baseTime)
	
	numLineItems := g.faker.IntRange(1, 10)
	lineItems := make([]LineItem, numLineItems)
	
	var totalAmount float64
	for i := 0; i < numLineItems; i++ {
		quantity := g.faker.IntRange(1, 5)
		unitPrice := g.faker.Price(10, 1000)
		lineItems[i] = LineItem{
			ID:          primitive.NewObjectID(),
			ProductID:   g.faker.UUID(),
			ProductName: g.faker.Product().Name,
			SKU:         g.faker.UUID(),
			Quantity:    quantity,
			UnitPrice:   unitPrice,
			TotalPrice:  unitPrice * float64(quantity),
			Category:    g.faker.Hobby(),
			Brand:       g.faker.Company(),
			Description: g.faker.Paragraph(2, 3, 5, " "),
		}
		totalAmount += lineItems[i].TotalPrice
	}
	
	// Add discounts
	numDiscounts := g.faker.IntRange(0, 2)
	discounts := make([]Discount, numDiscounts)
	for i := 0; i < numDiscounts; i++ {
		discounts[i] = Discount{
			ID:          primitive.NewObjectID(),
			Type:        g.faker.RandomString([]string{"percentage", "fixed"}),
			Code:        g.faker.UUID(),
			Amount:      g.faker.Float64Range(5, 50),
			Description: g.faker.Sentence(5),
		}
	}
	
	// Add taxes
	numTaxes := g.faker.IntRange(1, 3)
	taxes := make([]Tax, numTaxes)
	for i := 0; i < numTaxes; i++ {
		taxRate := g.faker.Float64Range(0.05, 0.15)
		taxes[i] = Tax{
			ID:          primitive.NewObjectID(),
			Type:        g.faker.RandomString([]string{"sales", "vat", "shipping"}),
			Rate:        taxRate,
			Amount:      totalAmount * taxRate,
			Description: g.faker.Sentence(5),
		}
	}
	
	status := g.faker.RandomString([]string{"pending", "processing", "shipped", "delivered", "cancelled"})
	var shippedDate, deliveredDate *time.Time
	if status == "shipped" || status == "delivered" {
		sd := g.faker.DateRange(orderDate, baseTime)
		shippedDate = &sd
	}
	if status == "delivered" {
		dd := g.faker.DateRange(orderDate, baseTime)
		deliveredDate = &dd
	}
	
	return Order{
		ID:            primitive.NewObjectID(),
		OrderNumber:   g.faker.UUID(),
		Status:        status,
		TotalAmount:   totalAmount,
		Currency:      g.faker.Currency().Short,
		OrderDate:     orderDate,
		ShippedDate:   shippedDate,
		DeliveredDate: deliveredDate,
		ShippingAddress: g.generateAddress(false),
		BillingAddress:  g.generateAddress(false),
		LineItems:     lineItems,
		Discounts:     discounts,
		Taxes:         taxes,
		Notes:         g.faker.Paragraph(1, 2, 5, " "),
		CreatedAt:     orderDate,
		UpdatedAt:     g.faker.DateRange(orderDate, baseTime),
	}
}

// generateMetadata creates random metadata
func (g *Generator) generateMetadata() map[string]interface{} {
	metadata := make(map[string]interface{})
	numEntries := g.faker.IntRange(5, 15)
	
	for i := 0; i < numEntries; i++ {
		key := g.faker.Word()
		// Mix of different value types
		switch g.faker.IntRange(0, 3) {
		case 0:
			metadata[key] = g.faker.LetterN(20)
		case 1:
			metadata[key] = g.faker.IntRange(1, 1000)
		case 2:
			metadata[key] = g.faker.Bool()
		case 3:
			metadata[key] = g.faker.Float64Range(0, 100)
		}
	}
	
	return metadata
}

// calculatePadding calculates the padding needed to reach target size
func (g *Generator) calculatePadding(doc *CustomerDocument) (string, error) {
	// Serialize the document without padding to measure its size
	doc.Padding = ""
	bsonData, err := bson.Marshal(doc)
	if err != nil {
		return "", err
	}
	
	currentSize := len(bsonData)
	targetSize := int(g.targetSize)
	
	// If already at or above target, return minimal padding
	if currentSize >= targetSize {
		return "", nil
	}
	
	// Calculate padding needed
	paddingNeeded := targetSize - currentSize
	
	// Use a compression-resistant pattern (random-looking but deterministic)
	// This helps with volume snapshotting tests
	paddingChar := "A"
	if paddingNeeded > 100 {
		// For larger padding, use a mix of characters to avoid compression
		paddingChar = "X"
	}
	
	// Generate padding string
	padding := make([]byte, paddingNeeded)
	for i := range padding {
		padding[i] = byte(paddingChar[0] + byte(i%26))
	}
	
	return string(padding), nil
}

// EstimateSize estimates the BSON size of a document without serializing
func EstimateSize(doc *CustomerDocument) int {
	// Rough estimation - actual size will be calculated during padding
	// This is used for quick checks
	return len(doc.CustomerID) + len(doc.Email) + len(doc.FirstName) + len(doc.LastName) + 500
}

