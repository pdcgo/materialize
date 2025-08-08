package exact_one

import (
	"encoding/json"
	"errors"

	"github.com/dgraph-io/badger/v3"
)

type ChangeExactOne interface {
	Exist(exist *bool) ChangeExactOne
	Before(data ExactHaveKey) ChangeExactOne
	Save() ChangeExactOne
	Delete() ChangeExactOne
	Err() error
}

type saveExactOneImpl struct {
	intx bool
	txn  *badger.Txn
	err  error
	newd ExactHaveKey
}

// Delete implements ChangeExactOne.
func (s *saveExactOneImpl) Delete() ChangeExactOne {
	err := s.txn.Delete([]byte(s.newd.Key()))
	if err != nil {
		return s.setErr(err)
	}
	return s
}

// Exist implements SaveExactOne.
func (s *saveExactOneImpl) Exist(exist *bool) ChangeExactOne {
	var err error
	_, err = s.txn.Get([]byte(s.newd.Key()))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			*exist = false
			return s
		}
		return s.setErr(err)
	}

	*exist = true
	return s
}

// Before implements SaveExactOne.
func (s *saveExactOneImpl) Before(data ExactHaveKey) ChangeExactOne {
	var err error
	item, err := s.txn.Get([]byte(s.newd.Key()))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return s
		}
		return s.setErr(err)
	}
	val, err := item.ValueCopy(nil)
	if err != nil {
		return s.setErr(err)
	}

	err = json.Unmarshal(val, data)
	return s.setErr(err)
}

// Err implements SaveExactOne.
func (s *saveExactOneImpl) Err() error {
	return s.err
}

// Save implements SaveExactOne.
func (s *saveExactOneImpl) Save() ChangeExactOne {
	var err error
	raw, err := json.Marshal(s.newd)
	if err != nil {
		return s.setErr(err)
	}

	err = s.txn.Set([]byte(s.newd.Key()), raw)
	if err != nil {
		return s.setErr(err)
	}

	if !s.intx {
		err = s.txn.Commit()
		return s.setErr(err)
	}

	return s
}

func (s *saveExactOneImpl) setErr(err error) *saveExactOneImpl {
	if s.err != nil {
		return s
	}

	if err != nil {
		if !s.intx {
			s.txn.Discard()
		}

		s.err = err
	}
	return s
}

func NewSaveExactOne(txn *badger.Txn, newd ExactHaveKey, intx bool) ChangeExactOne {
	return &saveExactOneImpl{
		intx: intx,
		txn:  txn,
		newd: newd,
	}
}
