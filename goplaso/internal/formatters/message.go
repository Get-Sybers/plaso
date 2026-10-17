package formatters

// GetMessageShort builds the short message, mirroring EventFormatter
// GetMessageShort for both basic and conditional formatters, including the
// ">80 → first 77 runes + '...'" truncation (counted in runes, as Python counts
// code points, not bytes).
func (f *Formatter) GetMessageShort(values map[string]any) string {
	var msg string
	if f.Type == "basic" {
		format := f.messagePieces[0]
		if len(f.shortPieces) > 0 && f.shortPieces[0] != "" {
			format = f.shortPieces[0]
		}
		msg = formatMessage(format, values)
	} else {
		if err := f.buildMaps(); err != nil {
			return ""
		}
		pieces, attrs := f.messagePieces, f.pieceAttrs
		if len(f.shortPieces) > 0 && !onlyEmpty(f.shortPieces) {
			pieces, attrs = f.shortPieces, f.shortPieceAttrs
		}
		msg = conditionalMessage(pieces, attrs, f.separator, values)
	}
	return truncateShort(msg)
}

// GetFormatStringAttributeNames returns the set of attribute names referenced by
// the (long) message format string, mirroring
// EventFormatter.GetFormatStringAttributeNames (used by l2tcsv's extra field).
// The result is constant per formatter, so it is computed once and shared; the
// returned map is read-only and callers copy any names they need into their own
// set, so concurrent read access from the worker pool is safe.
func (f *Formatter) GetFormatStringAttributeNames() map[string]bool {
	f.attrNamesOnce.Do(func() {
		names := map[string]bool{}
		for _, piece := range f.messagePieces {
			for _, m := range formatStringAttributeNameRE.FindAllStringSubmatch(piece, -1) {
				names[m[1]] = true
			}
		}
		f.attrNames = names
	})
	return f.attrNames
}

// truncateShort applies the Python len>80 → [:77]+"..." rule using rune counts.
func truncateShort(s string) string {
	if len([]rune(s)) <= 80 {
		return s
	}
	r := []rune(s)
	return string(r[:77]) + "..."
}

// PyStr exposes the Python str() conversion used for rendering raw attribute
// values (l2tcsv extra, rawpy, json fallbacks).
func PyStr(v any) string { return pyStr(v) }

// DefaultMessageShort mirrors DefaultEventFormatter.GetMessageShort: the
// "<DEFAULT> ..." form, truncated to 77 runes + "..." when longer than 80.
func DefaultMessageShort(values map[string]any) string {
	return truncateShort("<DEFAULT> " + defaultAttributeValues(values))
}

func onlyEmpty(pieces []string) bool {
	for _, p := range pieces {
		if p != "" {
			return false
		}
	}
	return true
}
