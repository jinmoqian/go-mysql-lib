package mysql

import "github.com/ruiaylin/sqlparser/parser"
import "github.com/ruiaylin/sqlparser/ast"
import "strings"
import "fmt"

// 简单的sql解析。因为没找到好用的模块。以后研究用mysql自带的解析
// 只需要解析create table中的表名、字段名（SET类型、UNSIGNED类型）

type TableColumn struct{
	Name       string
	ColumnType string   //类型，比如int、set等
	Unsigned   bool     //数值类是否是unsigned（true）
	SetParams  []string // 当类型是SET时，后台的参数
}
type Table struct{
	Schema string
	Name   string
	Cols   []*TableColumn
}
var columnTypes = []string{"geometry", "bit", "set", "enum", "date", "time", "year", "datetime", "timestamp", "varchar", "char", "tinyblob", "tinytext", "mediumblob", "mediumtext", "longblob", "longtext", "blob", "text", "tinyint", "smallint", "mediumint", "integer", "int", "bigint", "float", "double", "decimal"} // 注意顺序，如果一个类型A是另外一个类型B的前缀，需要把B放在前面
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
	fmt.Println("sqlparsed stmts=", stmts)
	for _, stmt := range stmts{
		if createTalbeStmt, ok := stmt.(*ast.CreateTableStmt); ok{
			tbl := &Table{}
			tbl.Schema = createTalbeStmt.Table.Schema.String()
			tbl.Name   = createTalbeStmt.Table.Name.String()
			tbl.Cols = make([]*TableColumn, 0)
			fmt.Println("sqlparsed Cols=", createTalbeStmt.Cols)
			for _, col := range createTalbeStmt.Cols{
				tableCol := &TableColumn{}
				tableCol.Name = col.Name.Name.String()
				for _, columnType := range columnTypes{
					if strings.HasPrefix(col.Tp.String(), columnType) {
						tableCol.ColumnType = columnType
						if tableCol.ColumnType == "set" || tableCol.ColumnType == "enum"{
							// 解析各个待选项（可能有重复），以下格式set('a','b','c')
							fmt.Println("SET 1 ==>", col.Tp.String())
							paramsStr := strings.TrimPrefix(col.Tp.String(), "set")
							paramsStr = strings.Trim(paramsStr, "()")
							fmt.Println("SET 2 ==>", paramsStr)
							params := strings.Split(paramsStr, ",")
							fmt.Println("SET 3 ==>", params)
							tableCol.SetParams = params
							fmt.Println("SET ==>", tableCol.SetParams)
						}else{
							// 判断是否有UNSIGNED
							tableCol.SetParams = nil
							tableCol.Unsigned = strings.Contains(col.Tp.String(), "UNSIGNED")
							fmt.Println("SEt sign")
						}
					}
				}
				tbl.Cols = append(tbl.Cols , tableCol)
				fmt.Println("TAbleCol=", tableCol)
			}
			ret = append(ret, tbl)
		}else{
			fmt.Println("SQL parse failed, SQL=", sql)
		}
	}
	return ret, err
}
