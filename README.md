# GORM Clickhouse Driver

## Quick Start

```go
import (
  "gorm.io/driver/clickhouse"
  "gorm.io/gorm"
)

// https://github.com/ClickHouse/clickhouse-go
dsn := "tcp://host:port?debug=true"
db, err := gorm.Open(clickhouse.Open(dsn), &gorm.Config{})
```

## Configuration

```go
import (
  "gorm.io/driver/clickhouse"
  "gorm.io/gorm"
)

	db, _ := gorm.Open(clickhouse.Open("tcp://192.168.31.220:9000?debug=true"),&gorm.Config{})
	err:= db.Set("gorm:table_options", "engine=Memory()").AutoMigrate(
			&User{},
		)
```


Checkout [https://gorm.io](https://gorm.io) for details.
