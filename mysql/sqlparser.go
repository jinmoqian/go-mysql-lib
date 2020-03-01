package mysql

import "github.com/ruiaylin/sqlparser/parser"
import "github.com/ruiaylin/sqlparser/ast"
import "strings"
//import "fmt"

// 简单的sql解析。因为没找到好用的模块。以后研究用mysql自带的解析
// 只需要解析create table中的表名、字段名（SET类型、UNSIGNED类型）
type PositionType int
const(
	AddColumnAtFirst PositionType = 1 // 新列加在开头
	AddColumnAfter   PositionType = 2 // 新列加在某一列后
	AddColumnAtTail  PositionType = 3 // 新列加在最尾	
)
type TableColumn struct{
	Name       string
	ColumnType string       //类型，比如int、set等
	Unsigned   bool         //数值类是否是unsigned（true）
	SetParams  []string     // 当类型是SET时，后台的参数
	Position   PositionType // 当alter table时，这列加在最前，还是在某列之后？
	AddAfter   string       // 当alter table时，如果指定加入的位置，把这列放进去
	Drop       bool         // 是否是drop一列。如果是alter，true/false。
}
type TableAction int
const(
	ActionCreate TableAction = 1 // create 表
	ActionAlter  TableAction = 2 // 修改表
)
type Table struct{
	Action TableAction
	Schema string
	Name   string
	Cols   []*TableColumn
}
// 注意顺序，如果一个类型A是另外一个类型B的前缀，需要把B放在前面
var columnTypes = []string{"geometry", "bit", "set", "enum", "date", "time", "year", "datetime", "timestamp", "varchar", "char", "tinyblob", "tinytext", "mediumblob", "mediumtext", "longblob", "longtext", "blob", "text", "tinyint", "smallint", "mediumint", "integer", "int", "bigint", "float", "double", "decimal"}
// 所有的可以有正负号，即DB中可以有UNSIGNED 的类型（value必须为true）
var numberColumnTypes = make(map[string] bool)
func init(){
	numberColumnTypes["tinyint"] = true
	numberColumnTypes["smallint"]= true
	numberColumnTypes["mediumint"] = true
	numberColumnTypes["integer"] = true
	numberColumnTypes["int"] = true
	numberColumnTypes["bigint"] = true
	numberColumnTypes["float"] = true
	numberColumnTypes["double"] = true
	numberColumnTypes["decimal"] = true
}
func parseColumnType(tableCol *TableColumn, col *ast.ColumnDef){
	for _, columnType := range columnTypes{
		if strings.HasPrefix(col.Tp.String(), columnType) {
			tableCol.ColumnType = columnType
			if tableCol.ColumnType == "set" || tableCol.ColumnType == "enum"{
				// 解析各个待选项（可能有重复），以下格式set('a','b','c')
				paramsStr := strings.TrimPrefix(col.Tp.String(), "set")
				paramsStr = strings.Trim(paramsStr, "()")
				params := strings.Split(paramsStr, ",")
				tableCol.SetParams = params
			}else{
				// 判断是否有UNSIGNED
				tableCol.SetParams = nil
				tableCol.Unsigned = strings.Contains(col.Tp.String(), "UNSIGNED")
			}
		}
	}
}
func parseSql(sql string) ([]*Table, error){
	parser := parser.New()
	stmts, err := parser.Parse(sql, "", "")
	//stmt, err := parser.Parse(sql)
	if err != nil{
		return nil, err
	}
	//stmts := make([]Statement, 1)
	//stmts[0] = stmt
	ret := make([]*Table, 0)
	for _, stmt := range stmts{
		if createTalbeStmt, ok := stmt.(*ast.CreateTableStmt); ok{
			tbl := &Table{}
			tbl.Action = ActionCreate
			tbl.Schema = createTalbeStmt.Table.Schema.String()
			tbl.Name   = createTalbeStmt.Table.Name.String()
			tbl.Cols = make([]*TableColumn, 0)
			for _, col := range createTalbeStmt.Cols{
				tableCol := &TableColumn{}
				tableCol.Name = col.Name.Name.String()
				parseColumnType(tableCol, col)
				tbl.Cols = append(tbl.Cols , tableCol)
			}
			ret = append(ret, tbl)
		}else if alterTableStmt, ok := stmt.(*ast.AlterTableStmt); ok{
			tbl := &Table{}
			tbl.Action = ActionAlter
			tbl.Schema = alterTableStmt.Table.Schema.String()
			tbl.Name = alterTableStmt.Table.Name.String()
			for _, spec := range alterTableStmt.Specs{
				tableCol := &TableColumn{}
				if spec.Tp == ast.AlterTableAddColumn{
					tableCol.Drop = false
					tableCol.Name = spec.Column.Name.Name.String()
					if spec.Position != nil{
						if spec.Position.Tp == ast.ColumnPositionFirst{
							tableCol.Position = AddColumnAtFirst
						}else{
							tableCol.Position = AddColumnAfter
							if spec.Position.RelativeColumn != nil{
								tableCol.AddAfter = spec.Position.RelativeColumn.Name.String()
							}else{
								// 指定了要加在某列后，却没给名字……
								continue
							}
						}
					}else{
						tableCol.Position = AddColumnAtTail						
					}
					parseColumnType(tableCol, spec.Column)
				}else if spec.Tp == ast.AlterTableDropColumn{
					tableCol.Drop = true
					tableCol.Name = spec.DropColumn.Name.String()
				}else{
					// 只处理这2种。其它的change和modify，因为模块的原因解析不了
					continue
				}
				tbl.Cols = append(tbl.Cols , tableCol)
			}
			ret = append(ret, tbl)
		}else{
			// 既不是create，又不是alter，或者解析不了，直接跳过
		}
	}
	return ret, err
}
