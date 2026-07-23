package variant

import (
	"errors"
	"fmt"
	"unicode/utf8"
)

// decodeGoValue decodes a value into the public Go representation returned by
// Unmarshal. Unlike Decode, it does not materialize a recursive Value tree.
func decodeGoValue(m Metadata, data []byte) (any, error) {
	if len(data) == 0 {
		return nil, errors.New("variant value: empty data")
	}

	header := data[0]
	basic := BasicType(header & 0x03)
	valueHeader := header >> 2

	switch basic {
	case BasicPrimitive:
		v, _, err := decodePrimitive(PrimitiveType(valueHeader), data[1:])
		if err != nil {
			return nil, err
		}
		return v.GoValue(), nil
	case BasicShortString:
		length := int(valueHeader)
		if len(data) < 1+length {
			return nil, fmt.Errorf("variant value: short string length %d exceeds data", length)
		}
		if !utf8.Valid(data[1 : 1+length]) {
			return nil, errors.New("variant value: short string is not valid UTF-8")
		}
		return string(data[1 : 1+length]), nil
	case BasicObject:
		return decodeGoObject(m, header, data[1:])
	case BasicArray:
		return decodeGoArray(m, header, data[1:])
	default:
		return nil, fmt.Errorf("variant value: unknown basic type %d", basic)
	}
}

func decodeGoObject(m Metadata, header byte, data []byte) (any, error) {
	// Object header byte layout (see encodeObject): bits 2-3 hold
	// field_offset_size_minus_one, bits 4-5 hold field_id_size_minus_one,
	// bit 6 holds is_large.
	offsetSzCode := (header >> 2) & 0x03
	fieldIDSizeCode := (header >> 4) & 0x03
	isLarge := (header >> 6) & 0x01

	fieldIDSize := offsetSize(fieldIDSizeCode)
	offsetSz := offsetSize(offsetSzCode)

	pos := 0

	// Read num_elements.
	var numElements int
	if isLarge == 1 {
		if len(data) < 4 {
			return nil, errors.New("variant value: not enough data for object num_elements")
		}
		numElements = int(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24)
		pos += 4
	} else {
		if len(data) < 1 {
			return nil, errors.New("variant value: not enough data for object num_elements")
		}
		numElements = int(data[0])
		pos++
	}

	// Match Decode's pre-allocation bounds check. It guarantees every field-ID
	// and non-terminal offset slot; the direct decoder reads those tables
	// in-place instead of retaining them.
	if remaining := len(data) - pos; numElements < 0 ||
		remaining/(fieldIDSize+offsetSz) < numElements {
		return nil, fmt.Errorf("variant value: object element count %d exceeds data", numElements)
	}

	fieldIDsStart := pos
	offsetsStart := fieldIDsStart + numElements*fieldIDSize
	lastOffsetPos := offsetsStart + numElements*offsetSz
	lastOffset, _, err := readUint(data[lastOffsetPos:], offsetSz)
	if err != nil {
		return nil, fmt.Errorf("variant value: reading object offset %d: %w", numElements, err)
	}
	valueDataStart := lastOffsetPos + offsetSz
	valueDataEnd := valueDataStart + lastOffset
	if valueDataEnd > len(data) || valueDataEnd < valueDataStart {
		return nil, errors.New("variant value: object value data exceeds input")
	}

	out := make(map[string]any, numElements)
	fieldIDPos := fieldIDsStart
	offsetPos := offsetsStart
	for i := range numElements {
		fieldID, _, _ := readUint(data[fieldIDPos:], fieldIDSize)
		fieldIDPos += fieldIDSize

		name, err := m.Lookup(fieldID)
		if err != nil {
			return nil, fmt.Errorf("variant value: object field %d: %w", i, err)
		}
		if _, duplicate := out[name]; duplicate {
			return nil, fmt.Errorf("variant value: duplicate object field %q", name)
		}

		offset, _, _ := readUint(data[offsetPos:], offsetSz)
		offsetPos += offsetSz
		valueStart := valueDataStart + offset
		if valueStart < valueDataStart || valueStart > valueDataEnd {
			return nil, fmt.Errorf("variant value: object field %d: invalid value offset", i)
		}

		value, err := decodeGoValue(m, data[valueStart:valueDataEnd])
		if err != nil {
			return nil, fmt.Errorf("variant value: object field %q: %w", name, err)
		}
		out[name] = value
	}

	return out, nil
}

func decodeGoArray(m Metadata, header byte, data []byte) (any, error) {
	offsetSzCode := (header >> 2) & 0x03
	isLarge := (header >> 4) & 0x01

	offsetSz := offsetSize(offsetSzCode)

	pos := 0

	// Read num_elements.
	var numElements int
	if isLarge == 1 {
		if len(data) < 4 {
			return nil, errors.New("variant value: not enough data for array num_elements")
		}
		numElements = int(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24)
		pos += 4
	} else {
		if len(data) < 1 {
			return nil, errors.New("variant value: not enough data for array num_elements")
		}
		numElements = int(data[0])
		pos++
	}

	// Match Decode's pre-allocation bounds check before making the final []any.
	if remaining := len(data) - pos; numElements < 0 ||
		remaining/offsetSz <= numElements {
		return nil, fmt.Errorf("variant value: array element count %d exceeds data", numElements)
	}

	offsetsStart := pos
	valueDataStart := offsetsStart + (numElements+1)*offsetSz

	// The count check above guarantees the complete offset table, so each
	// in-place read below has a supported width and sufficient input bytes.
	start, _, _ := readUint(data[offsetsStart:], offsetSz)
	offsetPos := offsetsStart + offsetSz
	out := make([]any, numElements)
	for i := range numElements {
		end, _, _ := readUint(data[offsetPos:], offsetSz)
		offsetPos += offsetSz

		elemStart := valueDataStart + start
		elemEnd := valueDataStart + end
		if elemStart < 0 || elemEnd > len(data) || elemStart > elemEnd {
			return nil, fmt.Errorf("variant value: array element %d: invalid offset", i)
		}

		value, err := decodeGoValue(m, data[elemStart:elemEnd])
		if err != nil {
			return nil, fmt.Errorf("variant value: array element %d: %w", i, err)
		}
		out[i] = value
		start = end
	}

	return out, nil
}
