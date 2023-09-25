package planner

import (
	"query"
	"record_manager"
)

type ProductScan struct {
	p1     Plan
	p2     Plan
	schema *record_manager.Schema
}

func NewProductScan(p1 Plan, p2 Plan) *ProductScan {
	product_scan := ProductScan{
		p1:     p1,
		p2:     p2,
		schema: record_manager.NewSchema(),
	}

	product_scan.schema.AddAll(p1.Schema())
	product_scan.schema.AddAll(p2.Schema())
	return &product_scan
}

func (p *ProductScan) Open() interface{} {
	s1 := p.p1.Open()
	s2 := p.p2.Open()
	return query.NewProductScan(s1.(query.Scan), s2.(query.Scan))
}

func (p *ProductScan) BlocksAccessed() int {
	return p.p1.BlocksAccessed() + (p.p1.RecordsOutput() * p.p2.BlocksAccessed())
}

func (p *ProductScan) DistinctValues(fldName string) int {
	if p.p1.Schema().HasFields(fldName) {
		return p.p1.DistinctValues(fldName)
	} else {
		return p.p2.DistinctValues(fldName)
	}
}

func (p *ProductScan) Schema() record_manager.SchemaInterface {
	return p.schema
}
