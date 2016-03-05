package svcrouter

import (
	"bytes"
	//	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
)

// type Envelope struct
//
// the Envelope structure holds routing information

type Envelope struct {
	Version    int               `json:"v"` // version of encoded header
	Attributes map[string]string `json:"a"` // attributes
}

const (
	EN_VERSION = 0 // for serialization future compatibility
)

const (
	EA_REQUESTID    = "ri" // unique ID of the request
	EA_ORIGINATORID = "oi" // oi - originator ID
	EA_SERVICETYPE  = "st" // hash(com.extl.blah)
	EA_SERVICEID    = "si" // instance of service (e.g. multiple clusters)
	EA_SHARDID      = "sh" // Thing to hash on (empty = random)
	EA_SERVERID     = "se" // se - ID of particular server
	EA_ENDPOINTID   = "ep" // ID of a thread (multiple threads on same endpoint)
)

// func NewEndpointEnvelope(serviceType, serviceId, shardId string) *Envelope
//
// Generate a new envelope for an endpoint

func NewEndpointEnvelope(serviceType, serviceId, shardId string) *Envelope {
	e := &Envelope{
		Version: EN_VERSION,
		Attributes: map[string]string{
			EA_SERVICETYPE: serviceType,
			EA_SERVICEID:   serviceId,
		},
	}
	if shardId != "" {
		e.Attributes[EA_SHARDID] = shardId
	}
	return e
}

// type AttributeFilter map[string]bool
//
// An AttributeFilter is a map specifying which attributes in an envelope are permitted and/or compulsory.

type AttributeFilter struct {
	Compulsory map[string]bool // set to true if compulsory, false if optional
}

// var ClientAttributeFilter
//
// Attribute filter for clients

var ClientAttributeFilter = &AttributeFilter{
	Compulsory: map[string]bool{
		EA_ORIGINATORID: false,
		EA_SERVICETYPE:  true,
		EA_SERVICEID:    false,
		EA_SHARDID:      false,
	},
}

// var EndpointAttributeFilter
//
// Attribute filter for endpoints

var EndpointAttributeFilter = &AttributeFilter{
	Compulsory: map[string]bool{
		EA_SERVICETYPE: true,
		EA_SERVICEID:   true,
		EA_SHARDID:     false,
	},
}

// var EndpointAttributeFilter
//
// Attribute filter for endpoints

var DispatcherAttributeFilter = &AttributeFilter{
	Compulsory: map[string]bool{
		EA_SERVICETYPE: true,
		EA_SERVICEID:   true,
		EA_SHARDID:     false,
	},
}

// func (e *Envelope) Has(k string) bool
//
// returns true if an envelope has a given key, else false

func (e *Envelope) Has(k string) bool {
	_, ok := e.Attributes[k]
	return ok
}

// func (e *Envelope) IsEqual(e2 *Envelope) bool
//
// returns true if an envelope has a given key, else false

func (e *Envelope) IsEqual(e2 *Envelope) bool {
	if e == nil {
		return e2 == nil
	} else if e2 == nil {
		return false
	} else if len(e.Attributes) != len(e2.Attributes) {
		return false
	} else if e.Version != e2.Version {
		return false
	} else {
		for k, v := range e.Attributes {
			if v2, ok := e2.Attributes[k]; !ok || v != v2 {
				return false
			}
		}
	}
	return true
}

// func (e *Envelope) Encode() ([]byte, error)
//
// Encode an envelope to its binary serialization

func (e *Envelope) Encode() ([]byte, error) {
	var buf bytes.Buffer
	//	enc := gob.NewEncoder(&buf)
	enc := json.NewEncoder(&buf)
	if err := enc.Encode(e); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// func DecodeEnvelope(data []byte) (*Envelope, error)
//
// Create an envelope from its binary serialisation

func DecodeEnvelope(data []byte) (*Envelope, error) {
	buf := bytes.NewBuffer(data)
	//	dec := gob.NewDecoder(buf)
	dec := json.NewDecoder(buf)
	var e Envelope
	if err := dec.Decode(&e); err != nil {
		return nil, err
	}
	return &e, nil
}

// func FindEnvelope(msg [][]byte) (*Envelope, int, error)
//
// Find an envelope within a message. Returns a pointer to the envelope and
// the index of the envelope frame

func FindEnvelope(msg [][]byte) (*Envelope, int, error) {
	for i, b := range msg {
		if len(b) == 0 && i < len(msg)-2 && len(msg[i+2]) == 0 {
			if e, err := DecodeEnvelope(msg[i+1]); err != nil {
				return nil, i + 1, err
			} else {
				return e, i + 1, nil
			}
		}
	}
	return nil, 0, errors.New("No envelope")
}

// func RemoveEnvelope(msg [][]byte) ([][]byte, *Envelope, error)
//
// Remove an envelope from a message. Returns the message with the envelope
// removed and the envelope.

func RemoveEnvelope(msg [][]byte) ([][]byte, *Envelope, error) {
	if e, i, err := FindEnvelope(msg); err != nil {
		return msg, nil, err
	} else {
		return append(msg[:i], msg[i+2:]...), e, nil
	}
}

// func InsertEnvelope(msg [][]byte, e *Envelope) ([][]byte, error)
//
// Insert an envelope into a message. Returns the message with the
// envelope inserted

func InsertEnvelope(msg [][]byte, e *Envelope) ([][]byte, error) {
	if envbytes, err := e.Encode(); err != nil {
		return nil, err
	} else {
		for i, b := range msg {
			if len(b) == 0 {
				x := [][]byte{[]byte{}, envbytes}
				return append(msg[:i], append(x, msg[i:]...)...), nil
			}
		}
		return nil, errors.New("No space for envelope")
	}
}

// func InsertLeadingDelimeter(msg [][]byte, e *Envelope) ([][]byte, error)
//
// Insert an envelope into a message. Returns the message with the
// envelope inserted

func InsertLeadingDelimeter(msg [][]byte) [][]byte {
	return append([][]byte{[]byte{}}, msg...)
}

// func NewEnvelopeFromMsg(msg [][]byte, filter *AttributeFilter) (*Envelope, error)
//
// Create an envelope from a message with a routing header, on the assumption that the innermost routing
// header has an ID that specifies an envelope in textual format

func NewEnvelopeFromMsg(msg [][]byte, filter *AttributeFilter) (*Envelope, error) {
	// the innermost address should have the ID set properly
	for i, b := range msg {
		log.Printf("Env: [%d][%d][%s]", i, len(b), string(b))
	}
	for i, b := range msg {
		if len(b) == 0 && i > 0 {
			attrs := strings.Split(string(msg[i-1]), " ")
			e := &Envelope{
				Version:    EN_VERSION,
				Attributes: make(map[string]string, len(attrs)),
			}
			for _, s := range attrs {
				kv := strings.SplitN(s, "=", 2)
				if len(kv) == 2 {
					e.Attributes[kv[0]] = kv[1]
				}
			}
			if filter != nil {
				if err := e.Validate(filter); err != nil {
					return nil, err
				}
			}
			return e, nil
		}
	}
	return nil, errors.New("Cannot find routing information")
}

// func (e *Envelope) Validate(filter *AttributeFilter) error
//
// Validate an envelope against an attribute filter

func (e *Envelope) Validate(filter *AttributeFilter) error {
	needed := make(map[string]bool)
	for k, v := range filter.Compulsory {
		if v {
			needed[k] = true
		}
	}
	for k, _ := range e.Attributes {
		if f, ok := filter.Compulsory[k]; !ok {
			return errors.New(fmt.Sprintf("Non-permitted attribute found: '%s'", k))
		} else if f {
			delete(needed, k)
		}
	}
	if len(needed) != 0 {
		n := []string{}
		for kk, _ := range needed {
			n = append(n, fmt.Sprintf("'%s'", kk))
		}
		return errors.New(fmt.Sprintf("Compulsory attribute(s) missing: %s", strings.Join(n, ", ")))
	}
	return nil
}

// type ComparableEnvelope struct
//
// A wrapper around Envelope that makes it comparable by defining a well-order relation

type ComparableEnvelope struct {
	sortedKeys sort.StringSlice
	Envelope
}

// func (ce *ComparableEnvelope) MakeSortOrder()
//
// Build up the sort order. We cache this for speed

func (ce *ComparableEnvelope) MakeSortOrder() {
	ce.sortedKeys = make([]string, len(ce.Attributes))
	ce.sortedKeys.Sort()
}

// func (ce *ComparableEnvelope) Invalidate()
//
// Invalidate the cached sorted key order

func (ce *ComparableEnvelope) Invalidate() {
	ce.sortedKeys = nil
}

// func (ce *ComparableEnvelope) EnsureSortOrder()
//
// Ensure there is a sort order

func (ce *ComparableEnvelope) EnsureSortOrder() {
	if ce.sortedKeys == nil {
		ce.MakeSortOrder()
	}
}

// func (ce *ComparableEnvelope) Compare (than *ComparableEnvelope) int
//
// Compare two envelopes. Return -1 if ce < than, 0 if ce = than, 1 if ce > than

func (ce *ComparableEnvelope) Compare(than *ComparableEnvelope) int {
	ce.EnsureSortOrder()
	than.EnsureSortOrder()
	if len(ce.sortedKeys) < len(than.sortedKeys) {
		return -1
	} else if len(ce.sortedKeys) > len(than.sortedKeys) {
		return 1
	}
	for i, k := range ce.sortedKeys {
		if k < than.sortedKeys[i] {
			return -1
		} else if k > than.sortedKeys[i] {
			return 1
		} else if ce.Attributes[k] < than.Attributes[k] {
			return -1
		} else if ce.Attributes[k] > than.Attributes[k] {
			return 1
		}
	}
	return 0
}

// func (ce *ComparableEnvelope) Less (than *ComparableEnvelope) bool
//
// Return true if ce < than according to the well ordering

func (ce *ComparableEnvelope) Less(than *ComparableEnvelope) bool {
	return ce.Compare(than) < 0
}

// func (ce *ComparableEnvelope) Equal (than *ComparableEnvelope) bool
//
// Return true if ce = than

func (ce *ComparableEnvelope) Equal(than *ComparableEnvelope) bool {
	return ce.Compare(than) == 0
}
