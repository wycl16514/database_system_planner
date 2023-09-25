在上一节我们说明了不同查询树其对应的执行效率不一样。给定 sql 语句，sql 解释器会构造出不同的查询树，因此我们需要专门计算哪种查询树具有最优效率，在数据库系统中，专门负责此工作的模块叫规划器，本节我们研究该模块的实现。

首先我们先给出规划器的接口，在项目目录下创建新文件夹 planner，在里面添加文件 interface.go，然后实现代码如下：
```go
package planner

import (
	"record_manager"
)

type Plan interface {
	Open() interface{}
	BlocksAccessed() int               //对应 B(s)
	RecordsOutput() int                //对应 R(s)
	DistinctValues(fldName string) int //对应 V(s,F)
	Schema() record_manager.SchemaInterface
}

```

Plan 接口对象跟我们前面的 Scan 对象很像，不同在于 Scan 对象接入表的数据，而 Plan 接口对象接入表的 meta data 数据。在后面的实现中，我们会针对 Select, Project, Product 等关系代数运算去创建对应的 Plan 接口对象，下面我们先看第一个 Plan 实例的实现，创建文件 table_plan.go，实现代码如下：
```go
package planner

import (
	metadata_manager "metadata_management"
	"query"
	"record_manager"
	"tx"
)

type TablePlan struct {
	tx      *tx.Transation
	tblName string
	layout  *record_manager.Layout
	si      *metadata_manager.StatInfo
}

func NewTablePlan(tx *tx.Transation, tblName string, md *metadata_manager.MetaDataManager) *TablePlan {
	tablePlanner := TablePlan{
		tx:      tx,
		tblName: tblName,
	}

	tablePlanner.layout = md.GetLayout(tablePlanner.tblName, tablePlanner.tx)
	tablePlanner.si = md.GetStatInfo(tblName, tablePlanner.layout, tx)

	return &tablePlanner
}

func (t *TablePlan) Open() interface{} {
	return query.NewTableScan(t.tx, t.tblName, t.layout)
}

func (t *TablePlan) RecordsOutput() int {
	return t.si.RecordsOutput()
}

func (t *TablePlan) BlocksAccessed() int {
	return t.si.BlocksAccessed()
}

func (t *TablePlan) DistinctValues(tblName string) int {
	return t.si.DistinctValues(tblName)
}

func (t *TablePlan) Schema() record_manager.SchemaInterface {
	return t.layout.Schema()
}

```
Plan 的实现在结构上与我们前面说过的 Scan 一样，最底层是 TablePlan，他直接返回对应数据库表的统计信息，实现 SelectPlan, ProjectPlan, ProductPlan 的时候需要传入一个 Plan 接口对象，他们相关接口的调用会转向调用输入 Plan 对象的接口。其中较为复杂的是 SelectPlan 的实现，因为它的执行依赖于传入的 Predicate 对象，其中我们以前在 Predicate 对象中实现的 ReductionFactor 接口就会被用于 RecordsAccessed，以便估计查询条件执行后所返回的数据库表缩小的程度，接口 EquatesWithConstant 用于 DistinctValues 以便用于检测 Predicate 对象对应的查询是否是"A=c"这种类型，其中 A 是字段名，c 是常量。

以前我们为了调试方便，在 Predicate和 Term 类的实现中注释掉了 ReductionFactor ，现在我们回去将他们反注释回来，，对于这两个函数的逻辑，我们将在代码的调试演示视频中再解释一下，相关视频请在 B 站搜索” coding 迪斯尼“。下面我们看 SelectPlan 的实现，创建文件 select_plan.go 实现代码如下：
```go
package planner

import (
	"query"
	"record_manager"
)

type SelectPlan struct {
	p    Plan
	pred *query.Predicate
}

func NewSelectPlan(p Plan, pred *query.Predicate) *SelectPlan {
	return &SelectPlan{
		p:    p,
		pred: pred,
	}
}

func (s *SelectPlan) Open() interface{} {
	scan := s.p.Open()
	return query.NewSelectionScan(scan.(query.UpdateScan), s.pred)
}

func (s *SelectPlan) BlocksAccessed() int {
	return s.p.BlocksAccessed()
}

func (s *SelectPlan) RecordsOutput() int {
	return s.p.RecordsOutput() / s.pred.ReductionFactor(s.p)
}

func (s *SelectPlan) min(a int, b int) int {
	if a <= b {
		return a
	}

	return b
}

func (s *SelectPlan) DistinctValues(fldName string) int {
	if s.pred.EquatesWithConstant(fldName) != nil {
		//如果查询是 A=c 类型，A 是字段，c 是常量，那么查询结果返回一条数据
		return 1
	} else {
		//如果查询是 A=B 类型，A,B 都是字段，那么查询结果返回不同类型数值较小的那个字段
		fldName2 := s.pred.EquatesWithField(fldName)
		if fldName2 != "" {
			return s.min(s.p.DistinctValues(fldName), s.p.DistinctValues(fldName2))
		} else {
			return s.p.DistinctValues(fldName)
		}
	}
}

func (s *SelectPlan) Schema() record_manager.SchemaInterface {
	return s.p.Schema()
}


```
可以看到 Plan 接口实例的实现跟前面 Scan 接口实例实现的逻辑差不多，很多接口要依赖于传入的 Plan 成员，下面我们看到的 ProjectPlan 跟 ProjectScan 如出一辙，相应接口就是调用到传入的 Plan 对象，创建 project_scan.go，输入代码如下：
```go
package planner

import (
	"query"
	"record_manager"
)

type ProjectPlan struct {
	p      Plan
	schema *record_manager.Schema
}

func NewProjectPlan(p Plan, fieldList []string) *ProjectPlan {
	project_plan := ProjectPlan{
		p:      p,
		schema: record_manager.NewSchema(),
	}

	for _, field := range fieldList {
		project_plan.schema.Add(field, project_plan.p.Schema())
	}

	return &project_plan
}

func (p *ProjectPlan) Open() interface{} {
	s := p.p.Open()
	return query.NewProjectScan(s.(query.Scan), p.schema.Fields())
}

func (p *ProjectPlan) BlocksAccessed() int {
	return p.p.BlocksAccessed()
}

func (p *ProjectPlan) RecordsOutput() int {
	return p.p.RecordsOutput()
}

func (p *ProjectPlan) DistinctValues(fldName string) int {
	return p.DistinctValues(fldName)
}

func (p *ProjectPlan) Schema() record_manager.SchemaInterface {
	return p.schema
}


```
最后我们看 ProductPlan 的实现，创建 product_plan.go 文件，实现代码如下：
```go
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

```
为了调用如上代码进行测试，我们完成测试代码如下，在 main.go 中输入如下代码：
```go
package main

import (
	bmg "buffer_manager"
	fm "file_manager"
	"fmt"
	lm "log_manager"
	metadata_manager "metadata_management"
	"planner"
	"query"
	"record_manager"
	"tx"
)

func printStats(n int, p planner.Plan) {
	fmt.Printf("Here are the stats for plan p %d\n", n)
	fmt.Printf("\tR(p%d):%d\n", n, p.RecordsOutput())
	fmt.Printf("\tB(p%d):%d\n", n, p.BlocksAccessed())
}

func createStudentTable() (*tx.Transation, *metadata_manager.MetaDataManager) {
	file_manager, _ := fm.NewFileManager("student", 2048)
	log_manager, _ := lm.NewLogManager(file_manager, "logfile.log")
	buffer_manager := bmg.NewBufferManager(file_manager, log_manager, 3)

	tx := tx.NewTransation(file_manager, log_manager, buffer_manager)
	sch := record_manager.NewSchema()

	sch.AddStringField("sname", 16)
	sch.AddIntField("majorId")
	sch.AddIntField("gradyear")
	layout := record_manager.NewLayoutWithSchema(sch)
	for _, field_name := range layout.Schema().Fields() {
		offset := layout.Offset(field_name)
		fmt.Printf("%s has offset %d\n", field_name, offset)
	}

	ts := query.NewTableScan(tx, "student", layout)
	fmt.Println("Filling the table with 50 random records")
	ts.BeforeFirst()
	val_for_field_sname := make([]int, 0)
	for i := 0; i < 50; i++ {
		ts.Insert() //指向一个可用插槽
		ts.SetInt("majorId", i)
		ts.SetInt("gradyear", 1990+i)
		val_for_field_sname = append(val_for_field_sname, i)
		s := fmt.Sprintf("sname_%d", i)
		ts.SetString("sname", s)
		fmt.Printf("inserting into slot %s: {%d , %s}\n", ts.GetRid().ToString(), i, s)
	}
	mdm := metadata_manager.NewMetaDataManager(false, tx)
	mdm.CreateTable("student", sch, tx)

	return tx, mdm
}

func main() {
	//构造 student 表
	tx, mdm := createStudentTable()
	p1 := planner.NewTablePlan(tx, "student", mdm)
	n := 10
	t := query.NewTerm(query.NewExpressionWithString("majorId"),
		query.NewExpressionWithConstant(query.NewConstantWithInt(&n)))
	pred := query.NewPredicateWithTerms(t)
	p2 := planner.NewSelectPlan(p1, pred)

	n1 := 2000
	t2 := query.NewTerm(query.NewExpressionWithString("gradyear"),
		query.NewExpressionWithConstant(query.NewConstantWithInt(&n1)))
	pred2 := query.NewPredicateWithTerms(t2)
	p3 := planner.NewSelectPlan(p1, pred2)

	c := make([]string, 0)
	c = append(c, "sname")
	c = append(c, "majorId")
	c = append(c, "gradyear")
	p4 := planner.NewProjectPlan(p3, c)

	printStats(1, p1)
	printStats(2, p2)
	printStats(3, p3)
	printStats(4, p4)
}

```
在上面代码中，我们创建了 student 表，他有三个字段分别为 sname, majorId, gradyear，然后我们创建 50 条记录插入表中，接下来我们创建 TablePlan, SelectPlan, ProjectPlan 来计算表中的查询数值，上面代码运行后输出结果如下：
```go
GOROOT=/usr/local/go #gosetup
GOPATH=/Users/my/go #gosetup
/usr/local/go/bin/go build -o /Users/my/Library/Caches/JetBrains/GoLand2023.2/tmp/GoLand/___1go_build_main_go /Users/my/Documents/b站代码/代码/simple_db/main.go #gosetup
/Users/my/Library/Caches/JetBrains/GoLand2023.2/tmp/GoLand/___1go_build_main_go
sname has offset 8
majorId has offset 32
gradyear has offset 40
Filling the table with 50 random records
inserting into slot [ 0 , 0 ]: {0 , sname_0}
inserting into slot [ 0 , 1 ]: {1 , sname_1}
inserting into slot [ 0 , 2 ]: {2 , sname_2}
inserting into slot [ 0 , 3 ]: {3 , sname_3}
inserting into slot [ 0 , 4 ]: {4 , sname_4}
inserting into slot [ 0 , 5 ]: {5 , sname_5}
inserting into slot [ 0 , 6 ]: {6 , sname_6}
inserting into slot [ 0 , 7 ]: {7 , sname_7}
inserting into slot [ 0 , 8 ]: {8 , sname_8}
inserting into slot [ 0 , 9 ]: {9 , sname_9}
inserting into slot [ 0 , 10 ]: {10 , sname_10}
inserting into slot [ 0 , 11 ]: {11 , sname_11}
inserting into slot [ 0 , 12 ]: {12 , sname_12}
inserting into slot [ 0 , 13 ]: {13 , sname_13}
inserting into slot [ 0 , 14 ]: {14 , sname_14}
inserting into slot [ 0 , 15 ]: {15 , sname_15}
inserting into slot [ 0 , 16 ]: {16 , sname_16}
inserting into slot [ 0 , 17 ]: {17 , sname_17}
inserting into slot [ 0 , 18 ]: {18 , sname_18}
inserting into slot [ 0 , 19 ]: {19 , sname_19}
inserting into slot [ 0 , 20 ]: {20 , sname_20}
inserting into slot [ 0 , 21 ]: {21 , sname_21}
inserting into slot [ 0 , 22 ]: {22 , sname_22}
inserting into slot [ 0 , 23 ]: {23 , sname_23}
inserting into slot [ 0 , 24 ]: {24 , sname_24}
inserting into slot [ 0 , 25 ]: {25 , sname_25}
inserting into slot [ 0 , 26 ]: {26 , sname_26}
inserting into slot [ 0 , 27 ]: {27 , sname_27}
inserting into slot [ 0 , 28 ]: {28 , sname_28}
inserting into slot [ 0 , 29 ]: {29 , sname_29}
inserting into slot [ 0 , 30 ]: {30 , sname_30}
inserting into slot [ 0 , 31 ]: {31 , sname_31}
inserting into slot [ 0 , 32 ]: {32 , sname_32}
inserting into slot [ 0 , 33 ]: {33 , sname_33}
inserting into slot [ 0 , 34 ]: {34 , sname_34}
inserting into slot [ 0 , 35 ]: {35 , sname_35}
inserting into slot [ 0 , 36 ]: {36 , sname_36}
inserting into slot [ 0 , 37 ]: {37 , sname_37}
inserting into slot [ 0 , 38 ]: {38 , sname_38}
inserting into slot [ 0 , 39 ]: {39 , sname_39}
inserting into slot [ 0 , 40 ]: {40 , sname_40}
inserting into slot [ 0 , 41 ]: {41 , sname_41}
inserting into slot [ 1 , 0 ]: {42 , sname_42}
inserting into slot [ 1 , 1 ]: {43 , sname_43}
inserting into slot [ 1 , 2 ]: {44 , sname_44}
inserting into slot [ 1 , 3 ]: {45 , sname_45}
inserting into slot [ 1 , 4 ]: {46 , sname_46}
inserting into slot [ 1 , 5 ]: {47 , sname_47}
inserting into slot [ 1 , 6 ]: {48 , sname_48}
inserting into slot [ 1 , 7 ]: {49 , sname_49}
Here are the stats for plan p 1
        R(p1):50
        B(p1):2
Here are the stats for plan p 2
        R(p2):2
        B(p2):2
Here are the stats for plan p 3
        R(p3):2
        B(p3):2
Here are the stats for plan p 4
        R(p4):2
        B(p4):2

Process finished with the exit code 0
```
从输出可以看出，50 条记录占据了两个区块，第一个区块存放了 41 条记录，第二个区块存放了 7 条记录，从 TablePlan 的输出我们看到 B(s)=2，也就是它表明数据库表有 2 个区块，R(s)=50，表内有 50 条记录，p2, p3 , p4 的输出我将在视频演示中进行讲解，请在 B 站搜索“Coding 迪斯尼"查看相关视频。
