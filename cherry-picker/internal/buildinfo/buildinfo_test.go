package buildinfo

import "testing"

func TestFingerprintStableAndSensitive(t *testing.T) {
	type input struct {
		Name string
		Rate int
	}
	a, err := Fingerprint(input{Name: "baseline", Rate: 300})
	if err != nil {
		t.Fatal(err)
	}
	b, err := Fingerprint(input{Name: "baseline", Rate: 300})
	if err != nil {
		t.Fatal(err)
	}
	c, err := Fingerprint(input{Name: "baseline", Rate: 301})
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("same config fingerprints differ: %q != %q", a, b)
	}
	if a == c {
		t.Fatal("different configs produced the same fingerprint")
	}
}
