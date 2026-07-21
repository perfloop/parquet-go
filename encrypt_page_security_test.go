package parquet_test

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/parquet-go/parquet-go"
)

type encryptedPageSecurityRow struct {
	Value []byte `parquet:"value"`
}

func writeEncryptedByteArrayPage(t *testing.T, keyByte byte, value []byte) []byte {
	t.Helper()

	key := aes128Key(keyByte)
	cfg := &parquet.EncryptionConfig{
		FooterKey:       key,
		EncryptedFooter: true,
		FileIdentifier:  []byte{keyByte, 1, 2, 3, 4, 5, 6, 7},
	}

	var data bytes.Buffer
	writer := parquet.NewGenericWriter[encryptedPageSecurityRow](&data,
		parquet.WithEncryption(cfg),
		parquet.PageBufferSize(4*1024),
		parquet.DefaultEncoding(&parquet.Plain),
	)
	if _, err := writer.Write([]encryptedPageSecurityRow{{Value: value}}); err != nil {
		t.Fatalf("write encrypted byte-array page: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close encrypted byte-array writer: %v", err)
	}
	return data.Bytes()
}

func writeEncryptedCompressedDictionary(t *testing.T, keyByte byte, value []byte) []byte {
	t.Helper()

	key := aes128Key(keyByte)
	cfg := &parquet.EncryptionConfig{
		FooterKey:       key,
		EncryptedFooter: true,
		FileIdentifier:  []byte{keyByte, 8, 7, 6, 5, 4, 3, 2},
	}

	var data bytes.Buffer
	writer := parquet.NewGenericWriter[encryptedPageSecurityRow](&data,
		parquet.WithEncryption(cfg),
		parquet.PageBufferSize(4*1024),
		parquet.DefaultEncodingFor(parquet.ByteArray, &parquet.RLEDictionary),
		parquet.Compression(&parquet.Gzip),
	)
	if _, err := writer.Write([]encryptedPageSecurityRow{{Value: value}}); err != nil {
		t.Fatalf("write encrypted dictionary page: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close encrypted dictionary writer: %v", err)
	}
	return data.Bytes()
}

func encryptedFilePages(t *testing.T, data []byte, keyByte byte) *parquet.FilePages {
	t.Helper()

	file, err := parquet.OpenFile(
		bytes.NewReader(data),
		int64(len(data)),
		parquet.WithDecryption(&staticKeyRetriever{footerKey: aes128Key(keyByte)}),
	)
	if err != nil {
		t.Fatalf("open encrypted file: %v", err)
	}
	pages, ok := file.RowGroups()[0].ColumnChunks()[0].Pages().(*parquet.FilePages)
	if !ok {
		t.Fatal("encrypted column did not return FilePages")
	}
	return pages
}

func encryptedByteArrayPages(t *testing.T, data []byte, keyByte byte) parquet.Pages {
	return encryptedFilePages(t, data, keyByte)
}

func TestEncryptedCompressedDictionaryReuseDoesNotExposePriorPlaintext(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previousProcs) })

	sentinel := []byte("victim-compressed-dictionary-secret")
	secret := append(bytes.Repeat([]byte("y"), 3*1024), sentinel...)
	victimData := writeEncryptedCompressedDictionary(t, 0xD5, secret)
	attackerValue := []byte("attacker-controlled")
	attackerData := writeEncryptedByteArrayPage(t, 0xD6, attackerValue)

	// Drop unrelated writer buffers so the attacker only races decrypted
	// dictionary buffers released by ReadDictionary below.
	runtime.GC()
	runtime.GC()

	victimPages := encryptedFilePages(t, victimData, 0xD5)
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

	for attempt := 0; attempt < 512; attempt++ {
		attackerPages := encryptedByteArrayPages(t, attackerData, 0xD6)
		attacker, err := attackerPages.ReadPage()
		if err != nil {
			t.Fatalf("read attacker page on attempt %d: %v", attempt, err)
		}

		attackerValues := attacker.Data()
		attackerPlaintext, offsets := attackerValues.Data()
		if len(offsets) != 2 || !bytes.Equal(attackerPlaintext, attackerValue) {
			t.Fatalf("attacker page on attempt %d did not decode its own plaintext", attempt)
		}
		if suffix := attackerPlaintext[len(attackerPlaintext):cap(attackerPlaintext)]; bytes.Contains(suffix, sentinel) {
			t.Fatalf("attacker page on attempt %d exposed plaintext from separately keyed victim dictionary", attempt)
		}

		parquet.Release(attacker)
		if err := attackerPages.Close(); err != nil {
			t.Fatalf("close attacker pages on attempt %d: %v", attempt, err)
		}
	}
}

func TestEncryptedPageReuseDoesNotExposePriorPlaintext(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previousProcs) })

	sentinel := []byte("victim-decrypted-page-secret")
	secret := append(bytes.Repeat([]byte("x"), 3*1024), sentinel...)
	victimData := writeEncryptedByteArrayPage(t, 0xD3, secret)
	attackerValue := []byte("attacker-controlled")
	attackerData := writeEncryptedByteArrayPage(t, 0xD4, attackerValue)

	// Drop unrelated writer buffers so the attacker only races the released
	// decrypted page body below, not setup allocations from constructing files.
	runtime.GC()
	runtime.GC()

	victimPages := encryptedByteArrayPages(t, victimData, 0xD3)
	victim, err := victimPages.ReadPage()
	if err != nil {
		t.Fatalf("read victim page: %v", err)
	}
	victimValues := victim.Data()
	victimPlaintext, _ := victimValues.Data()
	if !bytes.Contains(victimPlaintext, sentinel) {
		t.Fatal("victim page did not contain its plaintext sentinel")
	}
	parquet.Release(victim)
	if err := victimPages.Close(); err != nil {
		t.Fatalf("close victim pages: %v", err)
	}

	for attempt := 0; attempt < 512; attempt++ {
		attackerPages := encryptedByteArrayPages(t, attackerData, 0xD4)
		attacker, err := attackerPages.ReadPage()
		if err != nil {
			t.Fatalf("read attacker page on attempt %d: %v", attempt, err)
		}

		attackerValues := attacker.Data()
		attackerPlaintext, offsets := attackerValues.Data()
		if len(offsets) != 2 || !bytes.Equal(attackerPlaintext, attackerValue) {
			t.Fatalf("attacker page on attempt %d did not decode its own plaintext", attempt)
		}
		if suffix := attackerPlaintext[len(attackerPlaintext):cap(attackerPlaintext)]; bytes.Contains(suffix, sentinel) {
			t.Fatalf("attacker page on attempt %d exposed plaintext from separately keyed victim file", attempt)
		}

		parquet.Release(attacker)
		if err := attackerPages.Close(); err != nil {
			t.Fatalf("close attacker pages on attempt %d: %v", attempt, err)
		}
	}
}
