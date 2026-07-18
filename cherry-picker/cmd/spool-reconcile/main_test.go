package main

import "testing"

func TestValidateOperatorRefusesRootAndUnknownUID(t *testing.T) {
	for _, uid := range []string{"", " ", "0"} {
		if err := validateOperator(uid); err == nil {
			t.Fatalf("validateOperator(%q) accepted an unsafe identity", uid)
		}
	}
}

func TestValidateOperatorAcceptsServiceUID(t *testing.T) {
	if err := validateOperator("1001"); err != nil {
		t.Fatalf("validateOperator(service UID): %v", err)
	}
}
