package order_transaction

import "gorm.io/gorm"

type CrossProductAmount struct {
	TeamID uint
	Amount float64
}

type CreateOrderPayload struct {
	TeamID             uint
	WarehouseID        uint
	UserID             uint
	ShopID             uint
	OwnProductAmount   float64
	CrossProductAmount []*CrossProductAmount
}

type OrderTransaction interface {
	CreateOrder(payload *CreateOrderPayload) error
	WithdrawalOrder() error
	AdjustmentOrder() error
	ReturnOrder() error
	ProblemOrder() error
}

type orderTransactionImpl struct {
	tx *gorm.DB
}

// AdjustmentOrder implements OrderTransaction.
func (o *orderTransactionImpl) AdjustmentOrder() error {
	panic("unimplemented")
}

// CreateOrder implements OrderTransaction.
func (o *orderTransactionImpl) CreateOrder(payload *CreateOrderPayload) error {
	panic("unimplemented")
}

// ProblemOrder implements OrderTransaction.
func (o *orderTransactionImpl) ProblemOrder() error {
	panic("unimplemented")
}

// ReturnOrder implements OrderTransaction.
func (o *orderTransactionImpl) ReturnOrder() error {
	panic("unimplemented")
}

// WithdrawalOrder implements OrderTransaction.
func (o *orderTransactionImpl) WithdrawalOrder() error {
	panic("unimplemented")
}

func NewOrderTransaction(tx *gorm.DB) OrderTransaction {
	return &orderTransactionImpl{
		tx: tx,
	}
}
