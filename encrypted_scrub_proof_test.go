//go:build perfloop_scrub_proof

package parquet

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/rle"
)

// These helpers deliberately do not depend on candidate-owned external test
// helpers. This tagged proof source is retained with the large-page guard and
// is selected only for implementations that carry clearOnRelease.
type sealedScrubKeyRetriever struct{ footerKey []byte }

func (s *sealedScrubKeyRetriever) FooterKey(_ []byte) ([]byte, error) {
	return s.footerKey, nil
}

func (s *sealedScrubKeyRetriever) ColumnKey(_ []string, _ []byte) ([]byte, error) {
	return s.footerKey, nil
}

func sealedScrubAES128Key(b byte) []byte {
	key := make([]byte, 16)
	for i := range key {
		key[i] = b
	}
	return key
}

type sealedScrubByteArrayRow struct {
	Value []byte `parquet:"value"`
}

func sealedScrubWritePlainFile(t *testing.T, keyByte byte, values ...[]byte) []byte {
	t.Helper()

	key := sealedScrubAES128Key(keyByte)
	var data bytes.Buffer
	writer := NewGenericWriter[sealedScrubByteArrayRow](&data,
		WithEncryption(&EncryptionConfig{
			FooterKey:       key,
			EncryptedFooter: true,
			FileIdentifier:  []byte{keyByte, 1, 2, 3, 4, 5, 6, 7},
		}),
		PageBufferSize(4*1024),
		DefaultEncoding(&Plain),
	)
	rows := make([]sealedScrubByteArrayRow, len(values))
	for i := range values {
		rows[i].Value = values[i]
	}
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("write sealed plaintext proof page: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close sealed plaintext proof writer: %v", err)
	}
	return data.Bytes()
}

func sealedScrubWriteCompressedDictionary(t *testing.T, keyByte byte, value []byte) []byte {
	t.Helper()

	key := sealedScrubAES128Key(keyByte)
	var data bytes.Buffer
	writer := NewGenericWriter[sealedScrubByteArrayRow](&data,
		WithEncryption(&EncryptionConfig{
			FooterKey:       key,
			EncryptedFooter: true,
			FileIdentifier:  []byte{keyByte, 8, 7, 6, 5, 4, 3, 2},
		}),
		PageBufferSize(4*1024),
		DefaultEncodingFor(ByteArray, &RLEDictionary),
		Compression(&Gzip),
	)
	if _, err := writer.Write([]sealedScrubByteArrayRow{{Value: value}}); err != nil {
		t.Fatalf("write sealed dictionary proof page: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close sealed dictionary proof writer: %v", err)
	}
	return data.Bytes()
}

func sealedScrubFilePages(t *testing.T, data []byte, keyByte byte) *FilePages {
	t.Helper()

	file, err := OpenFile(
		bytes.NewReader(data),
		int64(len(data)),
		WithDecryption(&sealedScrubKeyRetriever{footerKey: sealedScrubAES128Key(keyByte)}),
	)
	if err != nil {
		t.Fatalf("open sealed scrub proof file: %v", err)
	}
	pages, ok := file.RowGroups()[0].ColumnChunks()[0].Pages().(*FilePages)
	if !ok {
		t.Fatal("encrypted proof column did not return FilePages")
	}
	return pages
}

func TestEncryptedPageReuseDoesNotExposePriorPlaintext(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previousProcs) })

	sentinel := []byte("victim-decrypted-page-secret")
	secret := append(bytes.Repeat([]byte("x"), 3*1024), sentinel...)
	victimData := sealedScrubWritePlainFile(t, 0xD3, secret)
	attackerValue := []byte("attacker-controlled")
	attackerData := sealedScrubWritePlainFile(t, 0xD4, attackerValue)

	runtime.GC()
	runtime.GC()

	victimPages := sealedScrubFilePages(t, victimData, 0xD3)
	victim, err := victimPages.ReadPage()
	if err != nil {
		t.Fatalf("read victim page: %v", err)
	}
	victimPlaintext, _ := victim.Data().Data()
	if !bytes.Contains(victimPlaintext, sentinel) {
		t.Fatal("victim page did not contain its plaintext sentinel")
	}
	Release(victim)
	if err := victimPages.Close(); err != nil {
		t.Fatalf("close victim pages: %v", err)
	}

	for range 512 {
		attackerPages := sealedScrubFilePages(t, attackerData, 0xD4)
		attacker, err := attackerPages.ReadPage()
		if err != nil {
			t.Fatalf("read attacker page: %v", err)
		}
		attackerPlaintext, offsets := attacker.Data().Data()
		if len(offsets) != 2 || !bytes.Equal(attackerPlaintext, attackerValue) {
			t.Fatal("attacker page did not decode its own plaintext")
		}
		if suffix := attackerPlaintext[len(attackerPlaintext):cap(attackerPlaintext)]; bytes.Contains(suffix, sentinel) {
			t.Fatal("attacker page exposed plaintext from separately keyed victim file")
		}
		Release(attacker)
		if err := attackerPages.Close(); err != nil {
			t.Fatalf("close attacker pages: %v", err)
		}
	}
}

func TestEncryptedCompressedDictionaryReuseDoesNotExposePriorPlaintext(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previousProcs) })

	sentinel := []byte("victim-compressed-dictionary-secret")
	secret := append(bytes.Repeat([]byte("y"), 3*1024), sentinel...)
	victimData := sealedScrubWriteCompressedDictionary(t, 0xD5, secret)
	attackerValue := []byte("attacker-controlled")
	attackerData := sealedScrubWritePlainFile(t, 0xD6, attackerValue)

	runtime.GC()
	runtime.GC()

	victimPages := sealedScrubFilePages(t, victimData, 0xD5)
	dictionary, err := victimPages.ReadDictionary()
	if err != nil {
		t.Fatalf("read victim dictionary: %v", err)
	}
	if dictionary == nil {
		t.Fatal("encrypted dictionary was not available")
	}
	if got := dictionary.Index(0).ByteArray(); !bytes.Equal(got, secret) {
		t.Fatal("victim dictionary did not contain its plaintext sentinel")
	}
	if err := victimPages.Close(); err != nil {
		t.Fatalf("close victim pages: %v", err)
	}

	for range 512 {
		attackerPages := sealedScrubFilePages(t, attackerData, 0xD6)
		attacker, err := attackerPages.ReadPage()
		if err != nil {
			t.Fatalf("read attacker page: %v", err)
		}
		attackerPlaintext, offsets := attacker.Data().Data()
		if len(offsets) != 2 || !bytes.Equal(attackerPlaintext, attackerValue) {
			t.Fatal("attacker page did not decode its own plaintext")
		}
		if suffix := attackerPlaintext[len(attackerPlaintext):cap(attackerPlaintext)]; bytes.Contains(suffix, sentinel) {
			t.Fatal("attacker page exposed plaintext from separately keyed victim dictionary")
		}
		Release(attacker)
		if err := attackerPages.Close(); err != nil {
			t.Fatalf("close attacker pages: %v", err)
		}
	}
}

func TestEncryptedPageReuseDoesNotExposePriorOffsets(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previousProcs) })

	victimValues := make([][]byte, 64)
	for i := range victimValues {
		victimValues[i] = bytes.Repeat([]byte{byte(i + 1)}, i+1)
	}
	victimData := sealedScrubWritePlainFile(t, 0xD8, victimValues...)
	attackerValue := bytes.Repeat([]byte("a"), 5*1024)
	attackerData := sealedScrubWritePlainFile(t, 0xD9, attackerValue)

	runtime.GC()
	runtime.GC()

	victimPages := sealedScrubFilePages(t, victimData, 0xD8)
	victim, err := victimPages.ReadPage()
	if err != nil {
		t.Fatalf("read victim page: %v", err)
	}
	_, victimOffsets := victim.Data().Data()
	if len(victimOffsets) != len(victimValues)+1 {
		t.Fatalf("victim page has %d offsets, want %d", len(victimOffsets), len(victimValues)+1)
	}
	privateOffsets := make(map[uint32]struct{}, len(victimOffsets)-1)
	for _, offset := range victimOffsets[1:] {
		if offset != 0 {
			privateOffsets[offset] = struct{}{}
		}
	}
	if len(privateOffsets) == 0 {
		t.Fatal("victim page did not have nonzero offsets")
	}
	Release(victim)
	if err := victimPages.Close(); err != nil {
		t.Fatalf("close victim pages: %v", err)
	}

	for range 512 {
		attackerPages := sealedScrubFilePages(t, attackerData, 0xD9)
		attacker, err := attackerPages.ReadPage()
		if err != nil {
			t.Fatalf("read attacker page: %v", err)
		}
		attackerPlaintext, attackerOffsets := attacker.Data().Data()
		if !bytes.Equal(attackerPlaintext, attackerValue) {
			t.Fatal("attacker page did not decode its own plaintext")
		}
		if len(attackerOffsets) != 2 {
			t.Fatalf("attacker page has %d offsets, want 2", len(attackerOffsets))
		}
		if cap(attackerOffsets) <= len(attackerOffsets) {
			t.Fatal("attacker offsets did not expose spare capacity")
		}
		for _, offset := range attackerOffsets[len(attackerOffsets):cap(attackerOffsets)] {
			if _, ok := privateOffsets[offset]; ok {
				t.Fatal("attacker page exposed offsets from separately keyed victim file")
			}
		}
		Release(attacker)
		if err := attackerPages.Close(); err != nil {
			t.Fatalf("close attacker pages: %v", err)
		}
	}
}

func TestSealedDecodeLevelsClearsBufferOnError(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previousProcs) })

	for _, test := range []struct {
		name string
		data []byte
	}{
		{
			name: "truncated header after a complete run",
			data: []byte{2, 1, 0x80},
		},
		{
			name: "complete run shorter than the declared value count",
			data: []byte{2, 1},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime.GC()
			runtime.GC()
			levels, err := decodeLevels(&rle.Encoding{BitWidth: 1}, 64, test.data, true)
			if err == nil {
				t.Fatal("decoding malformed levels succeeded")
			}
			if levels != nil {
				t.Fatal("decoding malformed levels retained its buffer")
			}

			reused := buffers.get(64)
			defer reused.unref()
			if got := reused.data.Slice()[0]; got != 0 {
				t.Fatalf("reused level buffer retained decoded value %d after error", got)
			}
		})
	}
}
