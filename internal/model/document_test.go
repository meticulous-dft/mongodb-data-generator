package model

import (
	"testing"
)

func TestDocumentGeneration(t *testing.T) {
	gen := NewGenerator(Size8KB)
	
	doc, err := gen.Generate()
	if err != nil {
		t.Fatalf("Failed to generate document: %v", err)
	}
	
	if doc == nil {
		t.Fatal("Generated document is nil")
	}
	
	if doc.CustomerID == "" {
		t.Error("CustomerID is empty")
	}
	
	if len(doc.Orders) == 0 {
		t.Error("No orders generated")
	}
	
	if len(doc.Addresses) == 0 {
		t.Error("No addresses generated")
	}
}

func TestDocumentSizes(t *testing.T) {
	sizes := []DocumentSize{Size2KB, Size4KB, Size8KB, Size16KB, Size32KB, Size64KB}
	
	for _, size := range sizes {
		t.Run(string(rune(size)), func(t *testing.T) {
			gen := NewGenerator(size)
			
			if gen.TargetSize() != size {
				t.Errorf("Expected target size %d, got %d", size, gen.TargetSize())
			}
			
			doc, err := gen.Generate()
			if err != nil {
				t.Fatalf("Failed to generate document: %v", err)
			}
			
			// Verify document has required fields
			if doc.CustomerID == "" {
				t.Error("CustomerID is empty")
			}
		})
	}
}

