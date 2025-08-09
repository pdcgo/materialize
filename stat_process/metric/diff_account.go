package metric

import (
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/materialize/stat_process/models"
	"github.com/pdcgo/materialize/stat_replica"
)

type DiffAccount struct {
	Day        string  `json:"day"`
	AccountID  uint    `json:"account_id"`
	TeamID     uint    `json:"team_id"`
	Amount     float64 `json:"amount"`
	DiffAmount float64 `json:"diff_amount"`
	NextKey    string  `json:"next_key"`
}

// Key implements exact_one.ExactHaveKey.
func (d *DiffAccount) Key() string {
	return fmt.Sprintf("exact/%s/%d", d.Day, d.AccountID)
}

type DiffChangeAccount struct {
	Day       string  `json:"day"`
	AccountID uint    `json:"account_id"`
	TeamID    uint    `json:"team_id"`
	Camount   float64 `json:"camount"`
}

type diffAccountCalcImpl struct {
	lateDays int
	exactOne exact_one.ExactlyOnce
	db       *badger.DB
}

func (d *diffAccountCalcImpl) ProcessCDC(cdata *stat_replica.CdcMessage) ([]*DiffChangeAccount, error) {
	var err error
	data := cdata.Data.(*models.BalanceAccountHistory)
	n := data.At

	accountID := data.AccountID
	amount := data.Amount

	teamID, typeID, err := d.getTeamID(accountID)
	if err != nil {
		return nil, err
	}

	changes := []*DiffChangeAccount{}

	// filter shopeepay
	if typeID != 7 {
		return changes, nil
	}

	dif := &DiffAccount{
		Day:       n.Format("2006-01-02"),
		AccountID: accountID,
		TeamID:    teamID,
		Amount:    amount,
	}
	var old *DiffAccount

	err = d.exactOne.Transaction(true, func(exact exact_one.ExactlyOnce) error {
		// search before
		err := d.iterateDay(n, func(day string) error {
			var oldfound, nextfound bool
			var err error
			olddif := &DiffAccount{
				Day:       day,
				AccountID: accountID,
				TeamID:    teamID,
			}
			oldfound, err = exact.GetItemStruct(olddif)
			if err != nil {
				return err
			}

			if !oldfound {
				return nil
			}

			old = olddif

			if olddif.NextKey != "" {
				nextdif := &DiffAccount{}
				nextfound, err = exact.GetItemStructKey(olddif.NextKey, nextdif)
				if nextfound {
					nextdif.DiffAmount = nextdif.Amount - dif.Amount
					dif.NextKey = nextdif.Key()
				}

				change, err := d.Save(exact, nextdif)
				if err != nil {
					return err
				}
				changes = append(changes, change)
			}

			dif.DiffAmount = dif.Amount - olddif.Amount
			olddif.NextKey = dif.Key()

			change, err := d.Save(exact, dif)
			if err != nil {
				return err
			}
			changes = append(changes, change)

			change, err = d.Save(exact, olddif)
			if err != nil {
				return err
			}
			changes = append(changes, change)
			return ErrStopIterate
		})
		if err != nil {
			return err
		}

		if old == nil {
			change, err := d.Save(exact, dif)
			if err != nil {
				return err
			}
			changes = append(changes, change)
			return err
		}
		return nil
	})

	return changes, err
}

func (d *diffAccountCalcImpl) Save(exact exact_one.ExactlyOnce, acc *DiffAccount) (*DiffChangeAccount, error) {
	change := DiffChangeAccount{
		Day:       acc.Day,
		AccountID: acc.AccountID,
		TeamID:    acc.TeamID,
		Camount:   0,
	}

	old := DiffAccount{}

	saver := exact.Change(acc)
	err := saver.
		Before(&old).
		Save().
		Err()

	camount := acc.DiffAmount - old.DiffAmount
	change.Camount = camount
	return &change, err

}

func (d *diffAccountCalcImpl) getTeamID(accountID uint) (uint, uint, error) {
	account := &models.ExpenseAccount{
		ID: accountID,
	}
	_, err := d.exactOne.GetItemStruct(account)
	return account.TeamID, account.AccountTypeID, err
}

var ErrStopIterate = errors.New("stop iterate")

func (d *diffAccountCalcImpl) iterateDay(n time.Time, handler func(day string) error) error {
	var err error

	for i := 1; i < 15; i++ {
		day := n.
			AddDate(0, 0, -i).
			Format("2006-01-02") // Subtract i days

		err = handler(day)
		if err != nil {
			if errors.Is(err, ErrStopIterate) {
				return nil
			}
			return err
		}
	}

	return nil
}

func NewDiffAccountCalc(db *badger.DB, exactOne exact_one.ExactlyOnce) *diffAccountCalcImpl {
	return &diffAccountCalcImpl{
		lateDays: 15,
		db:       db,
		exactOne: exactOne,
	}
}
