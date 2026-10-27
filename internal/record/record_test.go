package record

import "testing"

func TestCloneIsIndependent(t *testing.T) {
	r := New("orders.csv", 1)
	r.Fields["amount"] = "10"

	c := r.Clone()
	c.Fields["amount"] = "99"
	c.Fields["extra"] = true

	if r.Fields["amount"] != "10" {
		t.Errorf("original changed: amount = %v", r.Fields["amount"])
	}
	if _, ok := r.Get("extra"); ok {
		t.Error("original got the clone's new field")
	}
}

func TestCopyWithoutCloneSharesFields(t *testing.T) {
	r := New("orders.csv", 1)
	r.Fields["amount"] = "10"

	c := r
	c.Fields["amount"] = "99"

	if r.Fields["amount"] != "99" {
		t.Fatal("expected a plain copy to share the map")
	}
}
