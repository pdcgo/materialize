package materialize

import "gorm.io/gorm"

type View interface {
	GetDepend(view View) View
	Depends() []View
	Sql() string
	TableName() string
}

func CreateMaterialized(matdb *gorm.DB, view View) error {
	// Create dependencies first
	for _, dep := range view.Depends() {
		if err := CreateMaterialized(matdb, dep); err != nil {
			return err
		}
	}

	// Drop the materialized view if it exists
	dropSQL := "DROP MATERIALIZED VIEW IF EXISTS " + view.TableName()
	if err := matdb.Exec(dropSQL).Error; err != nil {
		return err
	}

	// Create the materialized view
	createSQL := "CREATE MATERIALIZED VIEW " + view.TableName() + " AS " + view.Sql()
	if err := matdb.Exec(createSQL).Error; err != nil {
		return err
	}

	return nil
}

type CsShopWDView struct {
	// cs_shop_wd
}

// Depends implements View.
func (c *CsShopWDView) Depends() []View {
	return nil
}

// GetDepend implements View.
func (c *CsShopWDView) GetDepend(view View) View {
	return nil
}

// Sql implements View.
func (c *CsShopWDView) Sql() string {
	return `
select 

    o.created_by_id as w_id,
    o.team_id as w_team_id,
    o.order_mp_id as w_mp_id,
    date(oa.fund_at AT TIME ZONE 'Asia/Jakarta') as w_day,
    
    sum(case when oa.type = 'order_fund' then oa.amount else 0 end) as wd_amount,
    sum(case when oa.type != 'order_fund' then oa.amount else 0 end) as wd_adj_amount



from public.order_adjustments oa
join public.orders o on o.id = oa.order_id 
where (o.is_partial is null or o.is_partial = false) 
	and (o.is_order_fake IS NULL OR o.is_order_fake = false)
group by w_id, w_team_id, w_mp_id, w_day
	`
}

// TableName implements View.
func (c *CsShopWDView) TableName() string {
	return "cs_shop_wd"
}

type CsShopSpentView struct{}

// Depends implements View.
func (c *CsShopSpentView) Depends() []View {
	return nil
}

// GetDepend implements View.
func (c *CsShopSpentView) GetDepend(view View) View {
	return nil
}

// Sql implements View.
func (c *CsShopSpentView) Sql() string {
	return `
select 
    it.create_by_id as c_id,
    it.team_id as s_team_id,
    o.order_mp_id as s_mp_id,
    date(it.created AT TIME ZONE 'Asia/Jakarta') as s_day,

    sum(o.item_count) as item_count,
    sum(o.total-(COALESCE(o.warehouse_fee, 0)+COALESCE(o.shipment_fee, 0))) as item_amount,
    sum(o.total) as spent_amount,
    sum(o.order_mp_total) as order_amount,
    count(o.id) as order_count

from public.inv_transactions it
join public.orders o on o.invertory_tx_id = it.id
where 
    it.type = 'order'
    and it.status != 'cancel'
    and (o.is_partial is null or o.is_partial = false) 
	  and (o.is_order_fake IS NULL OR o.is_order_fake = false)

group by c_id, s_team_id, s_mp_id, s_day
	
`
}

// TableName implements View.
func (c *CsShopSpentView) TableName() string {
	return "cs_shop_spent"
}

func UserShopSpentView(...View) View {
	return &CsShopSpentView{}
}

type EmptyView struct{}
