package clickhouse

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
	_ "github.com/ClickHouse/clickhouse-go"
	)

type Dialector struct {
	*Config
}

type Config struct {
	DriverName           string
	DSN                  string
	PreferSimpleProtocol bool
	Conn                 *sql.DB
}

func Open(dsn string) gorm.Dialector {
	return &Dialector{&Config{DSN: dsn,DriverName:"clickhouse"}}
}

func New(config Config) gorm.Dialector {
	return &Dialector{Config: &config}
}

func (dialector Dialector) Name() string {
	return "clickhouse"
}

func (dialector Dialector) Initialize(db *gorm.DB) (err error) {
	// register callbacks
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{
		WithReturning: true,
	})

	if dialector.Conn != nil {
		db.ConnPool = dialector.Conn
	} else if dialector.DriverName != "" {
		db.ConnPool, err = sql.Open(dialector.DriverName, dialector.Config.DSN)

	}
	return
}

func (dialector Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{migrator.Migrator{Config: migrator.Config{
		DB:                          db,
		Dialector:                   dialector,
		CreateIndexAfterCreateTable: true,
	}}}
}

func (dialector Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	return clause.Expr{SQL: "DEFAULT"}
}

func (dialector Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	writer.WriteByte('?')
}

func (dialector Dialector) QuoteTo(writer clause.Writer, str string) {
	writer.WriteByte('"')
	if strings.Contains(str, ".") {
		for idx, str := range strings.Split(str, ".") {
			if idx > 0 {
				writer.WriteString(`."`)
			}
			writer.WriteString(str)
			writer.WriteByte('"')
		}
	} else {
		writer.WriteString(str)
		writer.WriteByte('"')
	}
}

var numericPlaceholder = regexp.MustCompile("\\$(\\d+)")

func (dialector Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, numericPlaceholder, `'`, vars...)
}

func (dialector Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "bool"
	case schema.Uint:
		if field.AutoIncrement {
			switch {
			case field.Size < 16:
				return "UInt8 primary key autoincrement"
			case field.Size < 31:
				return "UInt16 primary key autoincrement"
			default:
				return "UInt32 primary key autoincrement"
			}
		} else {
			switch {
			case field.Size < 16:
				return "UInt8"
			case field.Size < 31:
				return "UInt16"
			default:
				return "UInt32"
			}
		}
	case schema.Int:
		if field.AutoIncrement {
			switch {
			case field.Size < 16:
				return "Int8 primary key autoincrement"
			case field.Size < 31:
				return "Int16 primary key autoincrement"
			default:
				return "Int32 primary key autoincrement"
			}
		} else {
			switch {
			case field.Size < 16:
				return "Int8"
			case field.Size < 31:
				return "Int16"
			default:
				return "Int32"
			}
		}
	case schema.Float:
		return "real"
	case schema.String:
		if field.Size > 0 {
			//return //fmt.Sprintf("varchar(%d)", field.Size)
			return fmt.Sprintf("FixedString(%d)", field.Size)
		}
		return "String"
	case schema.Time:
		//return "timestamptz"
		return "datetime"
	case schema.Bytes:
		return "blob"
	}
	fmt.Println("string(field.DataType)",string(field.DataType))
	return string(field.DataType)
}

func (dialectopr Dialector) SavePoint(tx *gorm.DB, name string) error {
	tx.Exec("SAVEPOINT " + name)
	return nil
}

func (dialectopr Dialector) RollbackTo(tx *gorm.DB, name string) error {
	tx.Exec("ROLLBACK TO SAVEPOINT " + name)
	return nil
}
